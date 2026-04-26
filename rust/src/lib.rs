//! Scantivy FFI surface — proto-encoded request/response over the JEP 442 FFM API.
//!
//! All functions take a request buffer (`*const u8`, `usize`) and write the response length
//! into `*mut usize`, returning a heap-allocated `*mut u8` that the caller MUST free via
//! `free_buffer(ptr, len)`.
//!
//! Safety: every entry point reads from `req_ptr` for `req_len` bytes via `unsafe` blocks; the
//! caller must guarantee the buffer is valid for that range. Wrapping the entry-point fns in
//! `unsafe` would force every call site to wrap in `unsafe { ... }` — the FFM/JNR bridging path
//! doesn't enforce that anyway, so we keep them as `extern "C" fn`s and assume the contract.
//!
//! Panic safety: every op body runs inside `std::panic::catch_unwind` so a Tantivy/Rust panic
//! lands as `error: Some("internal panic: …")` on the response message instead of unwinding into
//! the JVM (which would `abort()` the process under Rust 2024's `extern "C"` rules).

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
    // `Vec::with_capacity` and `extend_from_slice` (which prost uses internally) can panic on
    // allocation failure for huge responses. Wrap in `catch_unwind` so the JVM doesn't get
    // aborted — return null + len=0 instead. The Scala-side `Ffi.call` will see len=0, copy
    // zero bytes, and the parse step will fail with a decode error rather than crashing.
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let result = catch_unwind(AssertUnwindSafe(|| {
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
    }));
    result.unwrap_or_else(|_| {
        unsafe {
            if !out_len.is_null() {
                *out_len = 0;
            }
        }
        std::ptr::null_mut()
    })
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

/// Run an FFI op body, catching both `Result::Err` and panics. Returns a value of the response
/// type — known errors come through `on_err(error_string)`, panics come through the same
/// callback with a `"internal panic: <msg>"` prefix so callers can distinguish on the wire if
/// they want to.
fn run_ffi<T>(op: impl FnOnce() -> Result<T>, on_err: impl FnOnce(String) -> T) -> T {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    // The op closure captures raw pointers (req_ptr / out_len) which aren't `UnwindSafe` in
    // Rust's type system; raw pointers are inert across an unwind so AssertUnwindSafe is sound.
    match catch_unwind(AssertUnwindSafe(op)) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => on_err(err_to_string(e)),
        Err(payload) => on_err(format!("internal panic: {}", panic_message(payload))),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "<non-string panic payload>".to_string()
    }
}

// ============================================================================
// Index lifecycle
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn create_index(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::CreateIndexRequest>(req_ptr, req_len) }?;
            let id = index::create(&req)?;
            Ok(pb::CreateIndexResponse {
                index_id: Some(id),
                error: None,
            })
        },
        |e| pb::CreateIndexResponse {
            index_id: None,
            error: Some(e),
        },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn open_index(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::OpenIndexRequest>(req_ptr, req_len) }?;
            let id = index::open(&req.path, req.id_field.as_deref())?;
            Ok(pb::CreateIndexResponse {
                index_id: Some(id),
                error: None,
            })
        },
        |e| pb::CreateIndexResponse {
            index_id: None,
            error: Some(e),
        },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn drop_index(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::DropIndexRequest>(req_ptr, req_len) }?;
            index::drop_index(&req.index_id)?;
            Ok(pb::GenericResponse { error: None })
        },
        |e| pb::GenericResponse { error: Some(e) },
    );
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
    let resp = run_ffi(
        || {
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
        },
        |e| pb::GenericResponse { error: Some(e) },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn index_documents(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::IndexDocumentsRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            let writer = handle.writer.lock();
            for doc in &req.documents {
                let t = build_doc(&handle, doc)?;
                writer.add_document(t)?;
            }
            Ok(pb::GenericResponse { error: None })
        },
        |e| pb::GenericResponse { error: Some(e) },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn upsert_document(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = run_ffi(
        || {
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
        },
        |e| pb::GenericResponse { error: Some(e) },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn delete_document(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = run_ffi(
        || {
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
        },
        |e| pb::GenericResponse { error: Some(e) },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn delete_by_query(
    req_ptr: *const u8,
    req_len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let resp = run_ffi(
        || {
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
        },
        |e| pb::DeleteByQueryResponse {
            deleted_estimate: None,
            error: Some(e),
        },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn truncate(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::TruncateRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            let writer = handle.writer.lock();
            writer.delete_all_documents()?;
            Ok(pb::GenericResponse { error: None })
        },
        |e| pb::GenericResponse { error: Some(e) },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn optimize(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::OptimizeRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            let _ = req.max_segments;
            // Tantivy's IndexWriter has no public "merge to N segments now" — auto-merge runs on
            // commit. We trigger a commit so the merge policy runs over the current segment set.
            let mut writer = handle.writer.lock();
            writer.commit()?;
            Ok(pb::GenericResponse { error: None })
        },
        |e| pb::GenericResponse { error: Some(e) },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn commit(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::CommitRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            let mut writer = handle.writer.lock();
            let opstamp = writer.commit()?;
            Ok(pb::CommitResponse {
                opstamp: Some(opstamp),
                error: None,
            })
        },
        |e| pb::CommitResponse {
            opstamp: None,
            error: Some(e),
        },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn rollback(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::RollbackRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            let mut writer = handle.writer.lock();
            writer.rollback()?;
            Ok(pb::GenericResponse { error: None })
        },
        |e| pb::GenericResponse { error: Some(e) },
    );
    encode_response(&resp, out_len)
}

// ============================================================================
// Read ops
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn count(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::CountRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            let n = search::count(&handle, &req)?;
            Ok(pb::CountResponse {
                count: Some(n),
                error: None,
            })
        },
        |e| pb::CountResponse {
            count: None,
            error: Some(e),
        },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn search(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::SearchRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            search::search(&handle, &req)
        },
        |e| pb::SearchResponse {
            hits: vec![],
            total: None,
            facets: vec![],
            error: Some(e),
        },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn aggregate(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::AggregationRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            aggregate::aggregate(&handle, &req)
        },
        |e| pb::AggregationResponse {
            results: vec![],
            error: Some(e),
        },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn distinct(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::DistinctRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            distinct::distinct(&handle, &req)
        },
        |e| pb::DistinctResponse {
            values: vec![],
            next_cursor: None,
            error: Some(e),
        },
    );
    encode_response(&resp, out_len)
}

#[unsafe(no_mangle)]
pub extern "C" fn explain(req_ptr: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8 {
    let resp = run_ffi(
        || {
            let req = unsafe { read_request::<pb::ExplainRequest>(req_ptr, req_len) }?;
            let handle = index::get(&req.index_id)?;
            search::explain(&handle, &req)
        },
        |e| pb::ExplainResponse {
            explanation: String::new(),
            error: Some(e),
        },
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_ffi_converts_panics_into_error_responses() {
        // Trigger a panic inside the op closure and verify `run_ffi` catches it.
        let resp = run_ffi(
            || -> Result<pb::GenericResponse> {
                panic!("boom");
            },
            |e| pb::GenericResponse { error: Some(e) },
        );
        let err = resp.error.expect("expected error after panic");
        assert!(
            err.starts_with("internal panic: "),
            "expected `internal panic` prefix, got: {err}"
        );
        assert!(
            err.contains("boom"),
            "expected panic message in error, got: {err}"
        );
    }

    #[test]
    fn run_ffi_passes_through_successful_results() {
        let resp = run_ffi(
            || -> Result<pb::GenericResponse> { Ok(pb::GenericResponse { error: None }) },
            |_| pb::GenericResponse {
                error: Some("should not be called".into()),
            },
        );
        assert!(resp.error.is_none());
    }

    #[test]
    fn run_ffi_propagates_known_errors() {
        let resp = run_ffi(
            || -> Result<pb::GenericResponse> { Err(ScantivyError::IndexNotFound("nope".into())) },
            |e| pb::GenericResponse { error: Some(e) },
        );
        let err = resp.error.expect("expected error");
        assert!(err.contains("index not found"));
        assert!(!err.starts_with("internal panic: "));
    }
}
