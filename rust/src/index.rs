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
use tantivy::tokenizer::{
    Language, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer,
    TextAnalyzer, WhitespaceTokenizer,
};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

/// Register additional named tokenizers on `index` so schemas referencing them work out of the
/// box. Tantivy's `TokenizerManager::default()` ships with `raw`, `default`, and `en_stem`; we
/// add the most commonly-needed extras.
fn register_tokenizers(index: &Index) {
    let tokenizers = index.tokenizers();
    // `simple`: split on non-alphanumeric, no lowercasing. Useful when case is significant.
    tokenizers.register(
        "simple",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .build(),
    );
    // `whitespace`: split on whitespace, lowercased.
    tokenizers.register(
        "whitespace",
        TextAnalyzer::builder(WhitespaceTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .build(),
    );
    // `whitespace_raw`: split on whitespace, no case folding. For exact whitespace tokenization.
    tokenizers.register(
        "whitespace_raw",
        TextAnalyzer::builder(WhitespaceTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .build(),
    );
    // `lowercase`: keep the field as a single token but lowercased. Like `raw` + LowerCaser.
    tokenizers.register(
        "lowercase",
        TextAnalyzer::builder(RawTokenizer::default())
            .filter(LowerCaser)
            .build(),
    );
    // Edge n-grams keyed for autocomplete (3..=10). Apply LowerCaser so prefix matching is
    // case-insensitive.
    tokenizers.register(
        "ngram_3_10",
        TextAnalyzer::builder(NgramTokenizer::new(3, 10, true).expect("valid ngram bounds"))
            .filter(LowerCaser)
            .build(),
    );
    // Light English stemming over the default pipeline. Aliases Tantivy's built-in `en_stem`
    // for callers who prefer the more descriptive name.
    tokenizers.register(
        "english_stem",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(Stemmer::new(Language::English))
            .build(),
    );
}

const DEFAULT_WRITER_HEAP: usize = 50 * 1024 * 1024; // 50 MB

/// Initialize `env_logger` once per process so Tantivy's internal `log::*` calls surface to
/// stderr when the consumer sets `RUST_LOG`. `try_init` is a no-op if a logger is already
/// installed, so we never clobber a host-application logger.
static LOG_INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
fn init_logging() {
    LOG_INIT.get_or_init(|| {
        let _ = env_logger::try_init();
    });
}

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
    init_logging();
    let schema_def = req
        .schema
        .as_ref()
        .ok_or_else(|| ScantivyError::SchemaMismatch("missing schema".into()))?;
    let mut resolved = build_schema(schema_def)?;
    resolved.id_field = req.id_field.clone();
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
    register_tokenizers(&index);

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

pub fn open(path: &str, id_field: Option<&str>) -> Result<String> {
    init_logging();
    let abs = PathBuf::from(path).canonicalize().map_err(|e| {
        ScantivyError::SchemaMismatch(format!("cannot canonicalize path '{}': {}", path, e))
    })?;
    let id = sha_id(abs.to_string_lossy().as_ref());

    if REGISTRY.read().contains_key(&id) {
        return Ok(id);
    }

    let dir = MmapDirectory::open(&abs)?;
    let index = Index::open(dir)?;
    register_tokenizers(&index);
    let mut resolved_schema = read_schema(&index)?;
    // The id_field isn't part of Tantivy's on-disk schema, so callers must restate it when
    // reopening. Persist it on both the resolved schema (for query.rs / search.rs lookups) and
    // the IndexHandle (for upsert/delete id_term resolution).
    let id_field_owned = id_field.map(str::to_string);
    resolved_schema.id_field = id_field_owned.clone();
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
        id_field: id_field_owned,
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
        id_field: None,
    })
}
