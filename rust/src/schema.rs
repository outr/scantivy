use crate::error::{Result, ScantivyError};
use crate::pb;
use std::collections::{HashMap, HashSet};
use tantivy::schema::{
    BytesOptions, DateOptions, FAST, FacetOptions, Field, INDEXED, IndexRecordOption,
    IpAddrOptions, JsonObjectOptions, NumericOptions, STORED, Schema, SchemaBuilder,
    TextFieldIndexing, TextOptions,
};

/// Resolved schema with helper indexes for fast lookup during indexing/searching.
#[allow(dead_code)]
pub struct ResolvedSchema {
    pub schema: Schema,
    pub fields_by_name: HashMap<String, Field>,
    pub kinds: HashMap<Field, pb::FieldKind>,
    /// Text fields available to the default QueryParser (TEXT-kind only).
    pub text_fields: Vec<Field>,
    /// All FACET-kind fields (for facet-collector setup).
    pub facet_fields: HashSet<Field>,
}

impl ResolvedSchema {
    pub fn field(&self, name: &str) -> Result<Field> {
        self.fields_by_name
            .get(name)
            .copied()
            .ok_or_else(|| ScantivyError::MissingField(name.to_string()))
    }

    #[allow(dead_code)]
    pub fn kind(&self, field: Field) -> pb::FieldKind {
        self.kinds
            .get(&field)
            .copied()
            .unwrap_or(pb::FieldKind::Unspecified)
    }
}

pub fn build_schema(def: &pb::SchemaDef) -> Result<ResolvedSchema> {
    let mut builder = Schema::builder();
    let mut fields_by_name = HashMap::new();
    let mut kinds = HashMap::new();
    let mut text_fields = Vec::new();
    let mut facet_fields = HashSet::new();

    for fd in &def.fields {
        if fd.name.is_empty() {
            return Err(ScantivyError::SchemaMismatch(
                "field name must be non-empty".to_string(),
            ));
        }
        let kind = pb::FieldKind::try_from(fd.kind).unwrap_or(pb::FieldKind::Unspecified);
        let field = add_field(&mut builder, fd, kind)?;
        fields_by_name.insert(fd.name.clone(), field);
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
        schema: builder.build(),
        fields_by_name,
        kinds,
        text_fields,
        facet_fields,
    })
}

fn add_field(builder: &mut SchemaBuilder, fd: &pb::FieldDef, kind: pb::FieldKind) -> Result<Field> {
    let stored = fd.stored;
    let indexed = fd.indexed;
    let fast = fd.fast;
    let analyzer = fd.analyzer.as_deref().unwrap_or("default");
    let name = fd.name.as_str();

    let f = match kind {
        pb::FieldKind::Text => {
            let mut opts = TextOptions::default();
            if indexed {
                let ix = TextFieldIndexing::default()
                    .set_tokenizer(if fd.tokenized { analyzer } else { "raw" })
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions);
                opts = opts.set_indexing_options(ix);
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast(Some(analyzer));
            }
            builder.add_text_field(name, opts)
        }
        pb::FieldKind::String => {
            let mut opts = TextOptions::default();
            if indexed {
                let ix = TextFieldIndexing::default()
                    .set_tokenizer("raw")
                    .set_index_option(IndexRecordOption::Basic);
                opts = opts.set_indexing_options(ix);
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast(Some("raw"));
            }
            builder.add_text_field(name, opts)
        }
        pb::FieldKind::Bool => {
            let mut opts = NumericOptions::default();
            if indexed {
                opts = opts.set_indexed();
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast();
            }
            builder.add_bool_field(name, opts)
        }
        pb::FieldKind::I64 => {
            let mut opts = NumericOptions::default();
            if indexed {
                opts = opts.set_indexed();
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast();
            }
            builder.add_i64_field(name, opts)
        }
        pb::FieldKind::U64 => {
            let mut opts = NumericOptions::default();
            if indexed {
                opts = opts.set_indexed();
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast();
            }
            builder.add_u64_field(name, opts)
        }
        pb::FieldKind::F64 => {
            let mut opts = NumericOptions::default();
            if indexed {
                opts = opts.set_indexed();
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast();
            }
            builder.add_f64_field(name, opts)
        }
        pb::FieldKind::Date => {
            let mut opts = DateOptions::default();
            if indexed {
                opts = opts.set_indexed();
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast();
            }
            builder.add_date_field(name, opts)
        }
        pb::FieldKind::Bytes => {
            let mut opts = BytesOptions::default();
            if indexed {
                opts = opts.set_indexed();
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast();
            }
            builder.add_bytes_field(name, opts)
        }
        pb::FieldKind::Facet => {
            let mut opts = FacetOptions::default();
            if stored {
                opts = opts.set_stored();
            }
            builder.add_facet_field(name, opts)
        }
        pb::FieldKind::JsonField => {
            let mut opts = JsonObjectOptions::default();
            if stored {
                opts = opts.set_stored();
            }
            if indexed {
                let ix = TextFieldIndexing::default()
                    .set_tokenizer(if fd.tokenized { analyzer } else { "raw" })
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions);
                opts = opts.set_indexing_options(ix);
            }
            if fast {
                opts = opts.set_fast(Some(analyzer));
            }
            builder.add_json_field(name, opts)
        }
        pb::FieldKind::Ip => {
            let mut opts = IpAddrOptions::default();
            if indexed {
                opts = opts.set_indexed();
            }
            if stored {
                opts = opts.set_stored();
            }
            if fast {
                opts = opts.set_fast();
            }
            builder.add_ip_addr_field(name, opts)
        }
        pb::FieldKind::Unspecified => {
            return Err(ScantivyError::SchemaMismatch(format!(
                "field '{}' has unspecified kind",
                fd.name
            )));
        }
    };

    // STORED/INDEXED/FAST/FacetOptions::set_stored references silence unused-import warnings
    // since rustc doesn't see them as used through method paths.
    let _ = (STORED, INDEXED, FAST);
    Ok(f)
}
