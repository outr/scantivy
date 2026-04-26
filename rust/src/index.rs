//! Per-index handle, registry, and lifecycle ops.

use crate::error::{Result, ScantivyError};
use crate::pb;
use crate::schema::{ResolvedSchema, build_schema};
use parking_lot::{Mutex, RwLock};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use tantivy::directory::MmapDirectory;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

const DEFAULT_WRITER_HEAP: usize = 50 * 1024 * 1024; // 50 MB

pub struct IndexHandle {
    pub schema: ResolvedSchema,
    pub index: Index,
    pub writer: Mutex<IndexWriter<TantivyDocument>>,
    pub reader: IndexReader,
    pub id_field: Option<String>,
    /// Absolute path for on-disk indexes; None for in-memory.
    #[allow(dead_code)]
    pub path: Option<PathBuf>,
}

static REGISTRY: LazyLock<RwLock<HashMap<String, Arc<IndexHandle>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn get(id: &str) -> Result<Arc<IndexHandle>> {
    REGISTRY
        .read()
        .get(id)
        .cloned()
        .ok_or_else(|| ScantivyError::IndexNotFound(id.to_string()))
}

pub fn drop_index(id: &str) -> Result<()> {
    if let Some(handle) = REGISTRY.write().remove(id) {
        // Best-effort: commit any pending writes. The IndexWriter will drop naturally and
        // Tantivy will clean up merge threads on its own.
        if let Some(mut w) = handle.writer.try_lock() {
            let _ = w.commit();
        }
    }
    Ok(())
}

pub fn create(req: &pb::CreateIndexRequest) -> Result<String> {
    let schema_def = req
        .schema
        .as_ref()
        .ok_or_else(|| ScantivyError::SchemaMismatch("missing schema".into()))?;
    let resolved = build_schema(schema_def)?;
    let heap = req
        .writer_heap_bytes
        .map(|b| b as usize)
        .unwrap_or(DEFAULT_WRITER_HEAP);

    let (index, abs_path) = match req.path.as_deref() {
        Some(p) => {
            let path = PathBuf::from(p);
            std::fs::create_dir_all(&path)?;
            let dir = MmapDirectory::open(&path)?;
            let idx = Index::open_or_create(dir, resolved.schema.clone())?;
            (idx, Some(path.canonicalize().unwrap_or(path)))
        }
        None => (Index::create_in_ram(resolved.schema.clone()), None),
    };

    let writer = index.writer::<TantivyDocument>(heap)?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    let id = match &abs_path {
        Some(p) => sha_id(p.to_string_lossy().as_ref()),
        None => format!("mem-{}", uuid::Uuid::new_v4().simple()),
    };

    let handle = IndexHandle {
        schema: resolved,
        index,
        writer: Mutex::new(writer),
        reader,
        id_field: req.id_field.clone(),
        path: abs_path,
    };
    REGISTRY.write().insert(id.clone(), Arc::new(handle));
    Ok(id)
}

fn sha_id(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    let out = h.finalize();
    hex::encode(&out[..8]) // 16 hex chars
}

pub fn open(path: &str) -> Result<String> {
    let abs = PathBuf::from(path).canonicalize().map_err(|e| {
        ScantivyError::SchemaMismatch(format!("cannot canonicalize path '{}': {}", path, e))
    })?;
    let id = sha_id(abs.to_string_lossy().as_ref());

    if REGISTRY.read().contains_key(&id) {
        return Ok(id);
    }

    let dir = MmapDirectory::open(&abs)?;
    let index = Index::open(dir)?;
    let resolved_schema = read_schema(&index)?;
    let writer = index.writer::<TantivyDocument>(DEFAULT_WRITER_HEAP)?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    let handle = IndexHandle {
        schema: resolved_schema,
        index,
        writer: Mutex::new(writer),
        reader,
        id_field: None, // unknown when reattaching; caller must set via separate request later if needed
        path: Some(abs),
    };
    REGISTRY.write().insert(id.clone(), Arc::new(handle));
    Ok(id)
}

/// Reconstruct a `ResolvedSchema` from a Tantivy `Index`'s on-disk schema.
fn read_schema(index: &Index) -> Result<ResolvedSchema> {
    use std::collections::HashSet;
    use tantivy::schema::FieldType;

    let schema = index.schema();
    let mut fields_by_name = HashMap::new();
    let mut kinds = HashMap::new();
    let mut text_fields = Vec::new();
    let mut facet_fields = HashSet::new();

    for (field, entry) in schema.fields() {
        fields_by_name.insert(entry.name().to_string(), field);
        let kind = match entry.field_type() {
            FieldType::Str(o) => {
                if o.get_indexing_options().map(|i| i.tokenizer()) == Some("raw") {
                    pb::FieldKind::String
                } else {
                    pb::FieldKind::Text
                }
            }
            FieldType::I64(_) => pb::FieldKind::I64,
            FieldType::U64(_) => pb::FieldKind::U64,
            FieldType::F64(_) => pb::FieldKind::F64,
            FieldType::Bool(_) => pb::FieldKind::Bool,
            FieldType::Date(_) => pb::FieldKind::Date,
            FieldType::Bytes(_) => pb::FieldKind::Bytes,
            FieldType::Facet(_) => pb::FieldKind::Facet,
            FieldType::JsonObject(_) => pb::FieldKind::JsonField,
            FieldType::IpAddr(_) => pb::FieldKind::Ip,
        };
        kinds.insert(field, kind);
        match kind {
            pb::FieldKind::Text => text_fields.push(field),
            pb::FieldKind::Facet => {
                facet_fields.insert(field);
            }
            _ => {}
        }
    }

    Ok(ResolvedSchema {
        schema,
        fields_by_name,
        kinds,
        text_fields,
        facet_fields,
    })
}
