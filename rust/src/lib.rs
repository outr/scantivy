//! Scantivy FFI surface — proto-encoded request/response over JNR-FFI.
//!
//! All functions take a request buffer (`*const u8`, `usize`) and write the response length
//! into `*mut usize`, returning a heap-allocated `*mut u8` that the caller MUST free via
//! `free_buffer(ptr, len)`.
//!
//! Safety: every entry point reads from `req_ptr` for `req_len` bytes via `unsafe` blocks; the
//! caller must guarantee the buffer is valid for that range. Wrapping the entry-point fns in
//! `unsafe` would force every call site to wrap in `unsafe { ... }` — JNR's bridging path doesn't
//! enforce that anyway, so we keep them as `extern "C" fn`s and assume the contract.

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::convert::{append_field_value, datetime_from_millis};
use crate::error::{Result, ScantivyError};
use crate::index::IndexHandle;
use prost::Message;
use std::sync::Arc;
use tantivy::schema::{Facet, FieldType};
use tantivy::{TantivyDocument, Term};

mod aggregate;
mod convert;
mod distinct;
mod error;
mod index;
mod query;
mod schema;
mod search;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/scantivy.proto.rs"));
}

// ============================================================================
// FFI helpers
// ============================================================================

unsafe fn read_request<T: Message + Default>(req_ptr: *const u8, req_len: usize) -> Result<T> {
    if req_ptr.is_null() {
        return Err(ScantivyError::SchemaMismatch("null request buffer".into()));
    }
    let bytes = unsafe { std::slice::from_raw_parts(req_ptr, req_len) };
    Ok(T::decode(bytes)?)
}

fn encode_response<T: Message>(msg: &T, out_len: *mut usize) -> *mut u8 {
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf).expect("proto encoding failed");
    let len = buf.len();
    unsafe {
        if !out_len.is_null() {
            *out_len = len;
        }
    }
    let mut boxed = buf.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    ptr
}

#[unsafe(no_mangle)]
pub extern "C" fn free_buffer(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    unsafe {
        let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len));
    }
}

fn err_to_string(e: ScantivyError) -> String {
    e.to_string()
}

// ============================================================================
// Index lifecycle
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn create_index(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = match unsafe { read_request::<pb::CreateIndexRequest>(req_ptr, req_len) }
        .and_then(|req| index::create(&req))
    {
        Ok(id) => pb::CreateIndexResponse {
            index_id: Some(id),
            error: None,
        },
        Err(e) => pb::CreateIndexResponse {
            index_id: None,
            error: Some(err_to_string(e)),
        },
    };
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn open_index(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = match unsafe { read_request::<pb::OpenIndexRequest>(req_ptr, req_len) }
        .and_then(|req| index::open(&req.path))
    {
        Ok(id) => pb::CreateIndexResponse {
            index_id: Some(id),
            error: None,
        },
        Err(e) => pb::CreateIndexResponse {
            index_id: None,
            error: Some(err_to_string(e)),
        },
    };
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn drop_index(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = match unsafe { read_request::<pb::DropIndexRequest>(req_ptr, req_len) }
        .and_then(|req| index::drop_index(&req.index_id))
    {
        Ok(_) => pb::GenericResponse { error: None },
        Err(e) => pb::GenericResponse {
            error: Some(err_to_string(e)),
        },
    };
    encode_response(&resp, out_len)
}

// ============================================================================
// Write ops
// ============================================================================

fn build_doc(handle: &IndexHandle, doc: &pb::Document) -> Result<TantivyDocument> {
    let mut t = TantivyDocument::default();
    for fv in &doc.fields {
        append_field_value(&handle.schema, &mut t, fv)?;
    }
    Ok(t)
}

fn id_term(
    handle: &IndexHandle,
    id_field_override: &Option<String>,
    id_value: &pb::Value,
) -> Result<Term> {
    let field_name = id_field_override
        .as_deref()
        .or(handle.id_field.as_deref())
        .ok_or_else(|| {
            ScantivyError::SchemaMismatch("no id_field configured for upsert/delete".into())
        })?;
    convert::value_to_term(&handle.schema, field_name, id_value)
}

#[unsafe(no_mangle)]
pub extern "C" fn index_document(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = (|| -> Result<pb::GenericResponse> {
        let req = unsafe { read_request::<pb::IndexDocumentRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let doc = req
            .document
            .as_ref()
            .ok_or_else(|| ScantivyError::SchemaMismatch("missing document".into()))?;
        let t = build_doc(&handle, doc)?;
        let writer = handle.writer.lock();
        writer.add_document(t)?;
        Ok(pb::GenericResponse { error: None })
    })();
    let resp = resp.unwrap_or_else(|e| pb::GenericResponse {
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn index_documents(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = (|| -> Result<pb::GenericResponse> {
        let req = unsafe { read_request::<pb::IndexDocumentsRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let writer = handle.writer.lock();
        for doc in &req.documents {
            let t = build_doc(&handle, doc)?;
            writer.add_document(t)?;
        }
        Ok(pb::GenericResponse { error: None })
    })();
    let resp = resp.unwrap_or_else(|e| pb::GenericResponse {
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn upsert_document(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = (|| -> Result<pb::GenericResponse> {
        let req = unsafe { read_request::<pb::UpsertDocumentRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let id_value = req
            .id_value
            .as_ref()
            .ok_or_else(|| ScantivyError::SchemaMismatch("missing id_value".into()))?;
        let term = id_term(&handle, &req.id_field_override, id_value)?;
        let doc = req
            .document
            .as_ref()
            .ok_or_else(|| ScantivyError::SchemaMismatch("missing document".into()))?;
        let t = build_doc(&handle, doc)?;
        let writer = handle.writer.lock();
        writer.delete_term(term);
        writer.add_document(t)?;
        Ok(pb::GenericResponse { error: None })
    })();
    let resp = resp.unwrap_or_else(|e| pb::GenericResponse {
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn delete_document(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = (|| -> Result<pb::GenericResponse> {
        let req = unsafe { read_request::<pb::DeleteRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let id_value = req
            .id_value
            .as_ref()
            .ok_or_else(|| ScantivyError::SchemaMismatch("missing id_value".into()))?;
        let term = id_term(&handle, &req.id_field_override, id_value)?;
        let writer = handle.writer.lock();
        writer.delete_term(term);
        Ok(pb::GenericResponse { error: None })
    })();
    let resp = resp.unwrap_or_else(|e| pb::GenericResponse {
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn delete_by_query(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = (|| -> Result<pb::DeleteByQueryResponse> {
        let req = unsafe { read_request::<pb::DeleteByQueryRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let q = req
            .query
            .as_ref()
            .ok_or_else(|| ScantivyError::SchemaMismatch("missing query".into()))?;
        let compiled = query::compile(&handle.index, &handle.schema, q)?;
        let writer = handle.writer.lock();
        writer.delete_query(compiled)?;
        Ok(pb::DeleteByQueryResponse {
            deleted_estimate: None,
            error: None,
        })
    })();
    let resp = resp.unwrap_or_else(|e| pb::DeleteByQueryResponse {
        deleted_estimate: None,
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn truncate(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = (|| -> Result<pb::GenericResponse> {
        let req = unsafe { read_request::<pb::TruncateRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let writer = handle.writer.lock();
        writer.delete_all_documents()?;
        Ok(pb::GenericResponse { error: None })
    })();
    let resp = resp.unwrap_or_else(|e| pb::GenericResponse {
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn optimize(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = (|| -> Result<pb::GenericResponse> {
        let req = unsafe { read_request::<pb::OptimizeRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        // Tantivy doesn't expose "merge to N segments" directly; wait_merging_threads + ensuring
        // default merge policy converges is the closest stable equivalent.
        let _ = req.max_segments;
        // Tantivy's IndexWriter has no public "merge to N segments now" — auto-merge runs on commit.
        // We trigger a commit so the merge policy runs over the current segment set.
        let mut writer = handle.writer.lock();
        writer.commit()?;
        Ok(pb::GenericResponse { error: None })
    })();
    let resp = resp.unwrap_or_else(|e| pb::GenericResponse {
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn commit(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = (|| -> Result<pb::CommitResponse> {
        let req = unsafe { read_request::<pb::CommitRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let mut writer = handle.writer.lock();
        let opstamp = writer.commit()?;
        Ok(pb::CommitResponse {
            opstamp: Some(opstamp),
            error: None,
        })
    })();
    let resp = resp.unwrap_or_else(|e| pb::CommitResponse {
        opstamp: None,
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn rollback(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = (|| -> Result<pb::GenericResponse> {
        let req = unsafe { read_request::<pb::RollbackRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let mut writer = handle.writer.lock();
        writer.rollback()?;
        Ok(pb::GenericResponse { error: None })
    })();
    let resp = resp.unwrap_or_else(|e| pb::GenericResponse {
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

// ============================================================================
// Read ops
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn count(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = (|| -> Result<pb::CountResponse> {
        let req = unsafe { read_request::<pb::CountRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        let n = search::count(&handle, &req)?;
        Ok(pb::CountResponse {
            count: Some(n),
            error: None,
        })
    })();
    let resp = resp.unwrap_or_else(|e| pb::CountResponse {
        count: None,
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn search(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = (|| -> Result<pb::SearchResponse> {
        let req = unsafe { read_request::<pb::SearchRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        search::search(&handle, &req)
    })();
    let resp = resp.unwrap_or_else(|e| pb::SearchResponse {
        hits: vec![],
        total: None,
        facets: vec![],
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn aggregate(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = (|| -> Result<pb::AggregationResponse> {
        let req = unsafe { read_request::<pb::AggregationRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        aggregate::aggregate(&handle, &req)
    })();
    let resp = resp.unwrap_or_else(|e| pb::AggregationResponse {
        results: vec![],
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn distinct(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = (|| -> Result<pb::DistinctResponse> {
        let req = unsafe { read_request::<pb::DistinctRequest>(req_ptr, req_len) }?;
        let handle = index::get(&req.index_id)?;
        distinct::distinct(&handle, &req)
    })();
    let resp = resp.unwrap_or_else(|e| pb::DistinctResponse {
        values: vec![],
        next_cursor: None,
        error: Some(err_to_string(e)),
    });
    encode_response(&resp, out_len)
}

// Suppress unused-import diagnostics for items kept available to other modules.
#[allow(dead_code)]
fn _silence_unused() {
    let _ = (
        Facet::root(),
        datetime_from_millis(0),
        FieldType::Bool(Default::default()),
    );
    let _: Option<Arc<u8>> = None;
}
