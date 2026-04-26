//! Search execution + materialization.

use crate::convert::document_to_proto;
use crate::error::{Result, ScantivyError};
use crate::index::IndexHandle;
use crate::pb;
use crate::query::compile;
use std::collections::VecDeque;
use tantivy::collector::{
    Collector, Count, FacetCollector, MultiCollector, SegmentCollector, TopDocs,
};
use tantivy::columnar::{Column, StrColumn};
use tantivy::query::{AllQuery, Query};
use tantivy::schema::FieldType;
use tantivy::snippet::SnippetGenerator;
use tantivy::{
    DateTime, DocAddress, DocId, Order, Score, SegmentOrdinal, SegmentReader, TantivyDocument,
};

pub fn search(handle: &IndexHandle, req: &pb::SearchRequest) -> Result<pb::SearchResponse> {
    let q: Box<dyn Query> = match req.query.as_ref() {
        Some(q) => compile(&handle.index, &handle.schema, q)?,
        None => Box::new(AllQuery),
    };
    handle.reader.reload().ok();
    let searcher = handle.reader.searcher();

    let limit = req.limit.max(1) as usize;
    let offset = req.offset as usize;
    let max_docs = limit.saturating_add(offset);

    // Multi-key sort runs through a custom collector that keeps a tuple of sort values per doc.
    // For 0- and 1-key requests we keep the existing TopDocs fast paths, since Tantivy's heap-based
    // collectors are meaningfully cheaper than the collect-and-sort approach the multi-key path uses.
    let scored: Vec<(Score, DocAddress)> = if req.sort.len() <= 1 {
        let sort = req.sort.first().and_then(|s| s.clause.clone());
        match &sort {
            None => searcher.search(&q, &TopDocs::with_limit(max_docs).order_by_score())?,
            Some(pb::sort_clause::Clause::Relevance(r)) => {
                let docs = searcher.search(&q, &TopDocs::with_limit(max_docs).order_by_score())?;
                if matches!(
                    pb::SortDirection::try_from(r.direction),
                    Ok(pb::SortDirection::SortAsc)
                ) {
                    let mut v = docs;
                    v.reverse();
                    v
                } else {
                    docs
                }
            }
            Some(pb::sort_clause::Clause::IndexOrder(io)) => {
                let descending = matches!(
                    pb::SortDirection::try_from(io.direction),
                    Ok(pb::SortDirection::SortDesc)
                );
                let addrs: Vec<DocAddress> = searcher.search(
                    &q,
                    &IndexOrderCollector {
                        limit: max_docs,
                        descending,
                    },
                )?;
                addrs.into_iter().map(|a| (0.0_f32, a)).collect()
            }
            Some(pb::sort_clause::Clause::ByField(by)) => {
                let order = match pb::SortDirection::try_from(by.direction) {
                    Ok(pb::SortDirection::SortAsc) => Order::Asc,
                    _ => Order::Desc,
                };
                sort_by_fast_field(handle, q.as_ref(), &searcher, &by.field, order, max_docs)?
            }
        }
    } else {
        multi_sort_search(handle, q.as_ref(), &searcher, &req.sort, max_docs)?
    };

    // Total count via a separate pass (cheap on Tantivy's Count collector).
    let total = if req.count_total {
        Some(searcher.search(&q, &Count)? as u64)
    } else {
        None
    };

    // Facets via a separate MultiCollector pass.
    let facet_results = if !req.facets.is_empty() {
        run_facets(handle, q.as_ref(), &searcher, &req.facets)?
    } else {
        Vec::new()
    };

    // Snippet generators are query-scoped, not doc-scoped — build one per requested field, reuse
    // across hits.
    let snippet_generators =
        build_snippet_generators(handle, q.as_ref(), &searcher, &req.snippets)?;

    let mut hits: Vec<pb::SearchHit> = Vec::with_capacity(limit);
    for (score, addr) in scored.into_iter().skip(offset).take(limit) {
        if let Some(min) = req.min_doc_score
            && (score as f64) < min
        {
            continue;
        }
        let doc: TantivyDocument = searcher.doc(addr)?;
        let id_value = handle
            .id_field
            .as_deref()
            .and_then(|name| extract_id_string(handle, &doc, name).ok());
        let payload = build_payload(handle, &doc, req)?;
        let snippets = render_snippets(&snippet_generators, &doc);
        hits.push(pb::SearchHit {
            payload: Some(payload),
            score: if req.score_docs { Some(score) } else { None },
            id: id_value,
            snippets,
        });
    }

    Ok(pb::SearchResponse {
        hits,
        total,
        facets: facet_results,
        error: None,
    })
}

fn sort_by_fast_field(
    handle: &IndexHandle,
    q: &dyn Query,
    searcher: &tantivy::Searcher,
    field_name: &str,
    order: Order,
    max_docs: usize,
) -> Result<Vec<(Score, DocAddress)>> {
    let field = handle.schema.field(field_name)?;
    let entry = handle.schema.schema.get_field_entry(field);
    let fname = field_name.to_string();
    Ok(match entry.field_type() {
        FieldType::I64(_) => {
            let r: Vec<(Option<i64>, DocAddress)> = searcher.search(
                q,
                &TopDocs::with_limit(max_docs).order_by_fast_field::<i64>(fname, order),
            )?;
            r.into_iter().map(|(_, a)| (0.0_f32, a)).collect()
        }
        FieldType::U64(_) => {
            let r: Vec<(Option<u64>, DocAddress)> = searcher.search(
                q,
                &TopDocs::with_limit(max_docs).order_by_fast_field::<u64>(fname, order),
            )?;
            r.into_iter().map(|(_, a)| (0.0_f32, a)).collect()
        }
        FieldType::F64(_) => {
            let r: Vec<(Option<f64>, DocAddress)> = searcher.search(
                q,
                &TopDocs::with_limit(max_docs).order_by_fast_field::<f64>(fname, order),
            )?;
            r.into_iter().map(|(_, a)| (0.0_f32, a)).collect()
        }
        FieldType::Bool(_) => {
            let r: Vec<(Option<bool>, DocAddress)> = searcher.search(
                q,
                &TopDocs::with_limit(max_docs).order_by_fast_field::<bool>(fname, order),
            )?;
            r.into_iter().map(|(_, a)| (0.0_f32, a)).collect()
        }
        FieldType::Date(_) => {
            let r: Vec<(Option<tantivy::DateTime>, DocAddress)> = searcher.search(
                q,
                &TopDocs::with_limit(max_docs)
                    .order_by_fast_field::<tantivy::DateTime>(fname, order),
            )?;
            r.into_iter().map(|(_, a)| (0.0_f32, a)).collect()
        }
        FieldType::Str(_) => {
            let r: Vec<(Option<String>, DocAddress)> = searcher.search(
                q,
                &TopDocs::with_limit(max_docs).order_by_string_fast_field(fname, order),
            )?;
            r.into_iter().map(|(_, a)| (0.0_f32, a)).collect()
        }
        ft => {
            return Err(ScantivyError::UnsupportedFieldType(format!(
                "sort by field '{}' kind={:?} not yet supported",
                field_name, ft
            )));
        }
    })
}

fn run_facets(
    handle: &IndexHandle,
    q: &dyn Query,
    searcher: &tantivy::Searcher,
    requests: &[pb::FacetRequest],
) -> Result<Vec<pb::FacetResult>> {
    // One MultiCollector pass to gather all facet counts.
    let mut multi = MultiCollector::new();
    let mut handles = Vec::with_capacity(requests.len());
    for fr in requests {
        // Verify the field exists at request-build time.
        let _ = handle.schema.field(&fr.field)?;
        let mut fc = FacetCollector::for_field(&fr.field);
        let path_str = if fr.path.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", fr.path.join("/"))
        };
        fc.add_facet(path_str.as_str());
        handles.push((fr.clone(), multi.add_collector(fc)));
    }

    let mut fruit = searcher.search(q, &multi)?;

    let mut out = Vec::with_capacity(handles.len());
    for (fr, h) in handles {
        let counts = h.extract(&mut fruit);
        let path_str = if fr.path.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", fr.path.join("/"))
        };

        let mut total_children: u32 = 0;
        let mut total_docs: u32 = 0;
        for (_facet, count) in counts.get(path_str.as_str()) {
            total_children += 1;
            total_docs += count as u32;
        }

        let limit = fr.children_limit.unwrap_or(10) as usize;
        let raw: Vec<(&tantivy::schema::Facet, u64)> = counts.top_k(path_str.as_str(), limit);
        let entries: Vec<pb::FacetEntry> = raw
            .into_iter()
            .map(|(facet, count)| pb::FacetEntry {
                label: facet.to_path_string(),
                count: count as u32,
            })
            .collect();
        out.push(pb::FacetResult {
            field: fr.field,
            entries,
            total_count: total_docs,
            child_count: total_children,
        });
    }
    Ok(out)
}

fn extract_id_string(
    handle: &IndexHandle,
    doc: &TantivyDocument,
    id_field: &str,
) -> Result<String> {
    use tantivy::schema::OwnedValue;
    let field = handle.schema.field(id_field)?;
    for (f, v) in doc.field_values() {
        if f == field {
            let owned: OwnedValue = tantivy::schema::Value::as_value(&v).into();
            return Ok(match owned {
                OwnedValue::Str(s) => s,
                OwnedValue::Bytes(b) => hex::encode(b),
                OwnedValue::I64(n) => n.to_string(),
                OwnedValue::U64(n) => n.to_string(),
                _ => {
                    return Err(ScantivyError::UnsupportedFieldType(format!(
                        "id_field '{}' has unsupported type for echo",
                        id_field
                    )));
                }
            });
        }
    }
    Err(ScantivyError::MissingField(format!(
        "id field '{}' missing in stored doc",
        id_field
    )))
}

fn build_payload(
    handle: &IndexHandle,
    doc: &TantivyDocument,
    req: &pb::SearchRequest,
) -> Result<pb::search_hit::Payload> {
    let mode =
        pb::ConversionMode::try_from(req.conversion).unwrap_or(pb::ConversionMode::ConversionDoc);
    Ok(match mode {
        pb::ConversionMode::ConversionDoc => {
            pb::search_hit::Payload::Doc(document_to_proto(&handle.schema, doc)?)
        }
        pb::ConversionMode::ConversionValue => {
            let field_name = req.return_fields.first().ok_or_else(|| {
                ScantivyError::UnsupportedQuery("Conversion=VALUE requires return_fields[0]".into())
            })?;
            let pb_doc = document_to_proto(&handle.schema, doc)?;
            let value = pb_doc
                .fields
                .into_iter()
                .find(|fv| fv.name == *field_name)
                .and_then(|mut fv| fv.values.drain(..).next())
                .ok_or_else(|| {
                    ScantivyError::MissingField(format!(
                        "field '{}' missing from stored doc for VALUE conversion",
                        field_name
                    ))
                })?;
            pb::search_hit::Payload::Value(value)
        }
        pb::ConversionMode::ConversionJson => {
            let pb_doc = filter_doc(handle, doc, &req.return_fields)?;
            let json = doc_to_json_object(&pb_doc);
            let bytes = serde_json::to_vec(&json).unwrap();
            pb::search_hit::Payload::Json(pb::JsonValue { encoded: bytes })
        }
        pb::ConversionMode::ConversionMaterialized => {
            let pb_doc = filter_doc(handle, doc, &req.return_fields)?;
            pb::search_hit::Payload::Materialized(pb_doc)
        }
        pb::ConversionMode::ConversionDocAndIndexes => {
            pb::search_hit::Payload::Doc(document_to_proto(&handle.schema, doc)?)
        }
    })
}

fn filter_doc(
    handle: &IndexHandle,
    doc: &TantivyDocument,
    keep: &[String],
) -> Result<pb::Document> {
    let mut full = document_to_proto(&handle.schema, doc)?;
    if !keep.is_empty() {
        full.fields.retain(|fv| keep.contains(&fv.name));
    }
    Ok(full)
}

fn doc_to_json_object(doc: &pb::Document) -> serde_json::Value {
    use serde_json::Value as J;
    let mut map = serde_json::Map::new();
    for fv in &doc.fields {
        let arr: Vec<J> = fv.values.iter().map(value_to_json).collect();
        let v = if arr.len() == 1 {
            arr.into_iter().next().unwrap()
        } else {
            J::Array(arr)
        };
        map.insert(fv.name.clone(), v);
    }
    J::Object(map)
}

fn value_to_json(v: &pb::Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v.kind.as_ref() {
        Some(pb::value::Kind::StringValue(s)) => J::String(s.clone()),
        Some(pb::value::Kind::LongValue(n)) => J::from(*n),
        Some(pb::value::Kind::UlongValue(n)) => J::from(*n),
        Some(pb::value::Kind::DoubleValue(n)) => J::from(*n),
        Some(pb::value::Kind::BoolValue(b)) => J::from(*b),
        Some(pb::value::Kind::FacetValue(s)) => J::String(s.clone()),
        Some(pb::value::Kind::DateMillis(ms)) => J::from(*ms),
        Some(pb::value::Kind::BytesValue(b)) => J::String(hex::encode(b)),
        Some(pb::value::Kind::IpValue(s)) => J::String(s.clone()),
        Some(pb::value::Kind::JsonValue(jv)) => {
            serde_json::from_slice(&jv.encoded).unwrap_or(J::Null)
        }
        None => J::Null,
    }
}

/// Build one [`SnippetGenerator`] per requested spec. Generators are query-scoped, so we set them
/// up once and reuse across hits.
fn build_snippet_generators(
    handle: &IndexHandle,
    q: &dyn Query,
    searcher: &tantivy::Searcher,
    specs: &[pb::SnippetSpec],
) -> Result<Vec<(pb::SnippetSpec, SnippetGenerator)>> {
    let mut out = Vec::with_capacity(specs.len());
    for spec in specs {
        let field = handle.schema.field(&spec.field)?;
        let mut snippet_gen = SnippetGenerator::create(searcher, q, field)?;
        if let Some(max) = spec.max_num_chars {
            snippet_gen.set_max_num_chars(max as usize);
        }
        out.push((spec.clone(), snippet_gen));
    }
    Ok(out)
}

fn render_snippets(
    generators: &[(pb::SnippetSpec, SnippetGenerator)],
    doc: &TantivyDocument,
) -> Vec<pb::SnippetResult> {
    generators
        .iter()
        .map(|(spec, snippet_gen)| {
            let mut snippet = snippet_gen.snippet_from_doc(doc);
            let pre = spec.pre_tag.as_deref().unwrap_or("<b>");
            let post = spec.post_tag.as_deref().unwrap_or("</b>");
            snippet.set_snippet_prefix_postfix(pre, post);
            pb::SnippetResult {
                field: spec.field.clone(),
                fragment: snippet.to_html(),
            }
        })
        .collect()
}

pub fn count(handle: &IndexHandle, req: &pb::CountRequest) -> Result<u64> {
    let q = match req.query.as_ref() {
        Some(q) => compile(&handle.index, &handle.schema, q)?,
        None => Box::new(AllQuery),
    };
    handle.reader.reload().ok();
    let searcher = handle.reader.searcher();
    let n = searcher.search(&q, &Count)?;
    Ok(n as u64)
}

pub fn explain(handle: &IndexHandle, req: &pb::ExplainRequest) -> Result<pb::ExplainResponse> {
    let q = req
        .query
        .as_ref()
        .ok_or_else(|| ScantivyError::UnsupportedQuery("explain: missing query".into()))?;
    let compiled = compile(&handle.index, &handle.schema, q)?;
    let id_field = handle.schema.id_field.as_deref().ok_or_else(|| {
        ScantivyError::UnsupportedQuery("explain: schema must declare an id_field".into())
    })?;
    handle.reader.reload().ok();
    let searcher = handle.reader.searcher();
    match crate::query::lookup_doc_by_id(&handle.index, &handle.schema, id_field, &req.source_id)? {
        None => Ok(pb::ExplainResponse {
            explanation: String::new(),
            error: None,
        }),
        Some(addr) => {
            let explanation = compiled.explain(&searcher, addr)?;
            Ok(pb::ExplainResponse {
                explanation: explanation.to_pretty_json(),
                error: None,
            })
        }
    }
}

/// Collector that yields matching docs in index order (segment ordinal, then DocId).
/// `descending=true` walks segments and DocIds in reverse to give the last `limit` matches.
struct IndexOrderCollector {
    limit: usize,
    descending: bool,
}

struct SegIndexOrderCollector {
    segment_ord: SegmentOrdinal,
    limit: usize,
    descending: bool,
    docs: VecDeque<DocId>,
}

impl Collector for IndexOrderCollector {
    type Fruit = Vec<DocAddress>;
    type Child = SegIndexOrderCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        _reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(SegIndexOrderCollector {
            segment_ord: segment_local_id,
            limit: self.limit,
            descending: self.descending,
            docs: VecDeque::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<DocAddress>>,
    ) -> tantivy::Result<Vec<DocAddress>> {
        let mut out: Vec<DocAddress> = Vec::with_capacity(self.limit);
        let iter: Box<dyn Iterator<Item = Vec<DocAddress>>> = if self.descending {
            Box::new(segment_fruits.into_iter().rev())
        } else {
            Box::new(segment_fruits.into_iter())
        };
        for seg in iter {
            for da in seg {
                if out.len() >= self.limit {
                    return Ok(out);
                }
                out.push(da);
            }
        }
        Ok(out)
    }
}

impl SegmentCollector for SegIndexOrderCollector {
    type Fruit = Vec<DocAddress>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if self.descending {
            // Sliding tail: keep at most `limit` largest DocIds. Tantivy delivers DocIds in
            // ascending order per segment, so the tail is what we want.
            self.docs.push_back(doc);
            if self.docs.len() > self.limit {
                self.docs.pop_front();
            }
        } else if self.docs.len() < self.limit {
            self.docs.push_back(doc);
        }
    }

    fn harvest(self) -> Self::Fruit {
        let segment_ord = self.segment_ord;
        let docs: Box<dyn Iterator<Item = DocId>> = if self.descending {
            Box::new(self.docs.into_iter().rev())
        } else {
            Box::new(self.docs.into_iter())
        };
        docs.map(|d| DocAddress::new(segment_ord, d)).collect()
    }
}

// ============================================================================================
// Multi-key sort
// ============================================================================================

/// Per-key kind, resolved at query-build time from the schema. Each variant carries the field
/// name (for ByField kinds) so `for_segment` can look up the appropriate fast-field reader.
#[derive(Clone, Debug)]
enum KeyKind {
    Score,
    IndexOrder,
    I64(String),
    U64(String),
    F64(String),
    Bool(String),
    Date(String),
    Str(String),
}

/// Sort value for a single doc on a single key. Cross-kind comparisons never happen because all
/// docs in one search use the same key sequence; `None` represents a missing field value and is
/// ordered before every present value (so ascending sorts surface missing fields first).
///
/// Note on `Str`: Tantivy's str fast fields use *per-segment* term ordinals, so we have to
/// materialize the actual string here to get a globally-consistent comparison across segments.
#[derive(Clone, Debug)]
enum SortKeyValue {
    None,
    F32(f32),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    Str(String),
    Pos(u64),
}

fn cmp_value(a: &SortKeyValue, b: &SortKeyValue) -> std::cmp::Ordering {
    use SortKeyValue::*;
    use std::cmp::Ordering::*;
    match (a, b) {
        (None, None) => Equal,
        (None, _) => Less,
        (_, None) => Greater,
        (F32(x), F32(y)) => x.partial_cmp(y).unwrap_or(Equal),
        (I64(x), I64(y)) => x.cmp(y),
        (U64(x), U64(y)) => x.cmp(y),
        (F64(x), F64(y)) => x.partial_cmp(y).unwrap_or(Equal),
        (Bool(x), Bool(y)) => x.cmp(y),
        (Str(x), Str(y)) => x.cmp(y),
        (Pos(x), Pos(y)) => x.cmp(y),
        // Mixed kinds shouldn't occur — all docs in one search produce the same key sequence —
        // but we degrade gracefully rather than panic.
        _ => Equal,
    }
}

fn cmp_keys(a: &[SortKeyValue], b: &[SortKeyValue], ascending: &[bool]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for ((av, bv), &asc) in a.iter().zip(b.iter()).zip(ascending) {
        let ord = cmp_value(av, bv);
        let ord = if asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn build_keys(handle: &IndexHandle, sort: &[pb::SortClause]) -> Result<(Vec<KeyKind>, Vec<bool>)> {
    let mut kinds = Vec::with_capacity(sort.len());
    let mut ascending = Vec::with_capacity(sort.len());
    for clause in sort {
        let inner = clause
            .clause
            .as_ref()
            .ok_or_else(|| ScantivyError::UnsupportedQuery("empty sort clause".into()))?;
        let (kind, asc) = match inner {
            pb::sort_clause::Clause::Relevance(r) => (
                KeyKind::Score,
                matches!(
                    pb::SortDirection::try_from(r.direction),
                    Ok(pb::SortDirection::SortAsc)
                ),
            ),
            pb::sort_clause::Clause::IndexOrder(io) => (
                KeyKind::IndexOrder,
                matches!(
                    pb::SortDirection::try_from(io.direction),
                    Ok(pb::SortDirection::SortAsc)
                ),
            ),
            pb::sort_clause::Clause::ByField(by) => {
                let field = handle.schema.field(&by.field)?;
                let entry = handle.schema.schema.get_field_entry(field);
                let kind = match entry.field_type() {
                    FieldType::I64(_) => KeyKind::I64(by.field.clone()),
                    FieldType::U64(_) => KeyKind::U64(by.field.clone()),
                    FieldType::F64(_) => KeyKind::F64(by.field.clone()),
                    FieldType::Bool(_) => KeyKind::Bool(by.field.clone()),
                    FieldType::Date(_) => KeyKind::Date(by.field.clone()),
                    FieldType::Str(_) => KeyKind::Str(by.field.clone()),
                    ft => {
                        return Err(ScantivyError::UnsupportedFieldType(format!(
                            "sort by field '{}' kind={:?} not supported in multi-sort",
                            by.field, ft
                        )));
                    }
                };
                let asc = matches!(
                    pb::SortDirection::try_from(by.direction),
                    Ok(pb::SortDirection::SortAsc)
                );
                (kind, asc)
            }
        };
        kinds.push(kind);
        ascending.push(asc);
    }
    Ok((kinds, ascending))
}

pub fn multi_sort_search(
    handle: &IndexHandle,
    q: &dyn Query,
    searcher: &tantivy::Searcher,
    sort: &[pb::SortClause],
    limit: usize,
) -> Result<Vec<(Score, DocAddress)>> {
    let (kinds, ascending) = build_keys(handle, sort)?;
    let collector = MultiSortCollector {
        kinds,
        ascending,
        limit,
    };
    let result = searcher.search(q, &collector)?;
    Ok(result)
}

struct MultiSortCollector {
    kinds: Vec<KeyKind>,
    ascending: Vec<bool>,
    limit: usize,
}

enum KeyExtractor {
    Score,
    IndexOrder(SegmentOrdinal),
    I64(Column<i64>),
    U64(Column<u64>),
    F64(Column<f64>),
    Bool(Column<bool>),
    Date(Column<DateTime>),
    Str(StrColumn),
}

struct SegMultiSortCollector {
    extractors: Vec<KeyExtractor>,
    ascending: Vec<bool>,
    limit: usize,
    segment_ord: SegmentOrdinal,
    matches: Vec<(Vec<SortKeyValue>, DocAddress, f32)>,
}

impl Collector for MultiSortCollector {
    type Fruit = Vec<(Score, DocAddress)>;
    type Child = SegMultiSortCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let ff = reader.fast_fields();
        let mut extractors = Vec::with_capacity(self.kinds.len());
        for kind in &self.kinds {
            let ext = match kind {
                KeyKind::Score => KeyExtractor::Score,
                KeyKind::IndexOrder => KeyExtractor::IndexOrder(segment_local_id),
                KeyKind::I64(name) => KeyExtractor::I64(ff.i64(name)?),
                KeyKind::U64(name) => KeyExtractor::U64(ff.u64(name)?),
                KeyKind::F64(name) => KeyExtractor::F64(ff.f64(name)?),
                KeyKind::Bool(name) => KeyExtractor::Bool(ff.bool(name)?),
                KeyKind::Date(name) => KeyExtractor::Date(ff.date(name)?),
                KeyKind::Str(name) => {
                    let col = ff.str(name)?.ok_or_else(|| {
                        tantivy::TantivyError::SchemaError(format!(
                            "field '{name}' is not a STR fast field"
                        ))
                    })?;
                    KeyExtractor::Str(col)
                }
            };
            extractors.push(ext);
        }
        Ok(SegMultiSortCollector {
            extractors,
            ascending: self.ascending.clone(),
            limit: self.limit,
            segment_ord: segment_local_id,
            matches: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.kinds.iter().any(|k| matches!(k, KeyKind::Score))
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut all: Vec<(Vec<SortKeyValue>, DocAddress, f32)> =
            segment_fruits.into_iter().flatten().collect();
        all.sort_by(|a, b| cmp_keys(&a.0, &b.0, &self.ascending));
        all.truncate(self.limit);
        Ok(all
            .into_iter()
            .map(|(_, addr, score)| (score, addr))
            .collect())
    }
}

impl SegmentCollector for SegMultiSortCollector {
    /// Per-segment fruit carries the key tuple so `merge_fruits` can re-rank globally without
    /// re-extracting from fast fields.
    type Fruit = Vec<(Vec<SortKeyValue>, DocAddress, f32)>;

    fn collect(&mut self, doc: DocId, score: f32) {
        let mut key = Vec::with_capacity(self.extractors.len());
        for ext in &self.extractors {
            key.push(match ext {
                KeyExtractor::Score => SortKeyValue::F32(score),
                KeyExtractor::IndexOrder(seg) => {
                    SortKeyValue::Pos(((*seg as u64) << 32) | doc as u64)
                }
                KeyExtractor::I64(c) => c
                    .first(doc)
                    .map(SortKeyValue::I64)
                    .unwrap_or(SortKeyValue::None),
                KeyExtractor::U64(c) => c
                    .first(doc)
                    .map(SortKeyValue::U64)
                    .unwrap_or(SortKeyValue::None),
                KeyExtractor::F64(c) => c
                    .first(doc)
                    .map(SortKeyValue::F64)
                    .unwrap_or(SortKeyValue::None),
                KeyExtractor::Bool(c) => c
                    .first(doc)
                    .map(SortKeyValue::Bool)
                    .unwrap_or(SortKeyValue::None),
                KeyExtractor::Date(c) => c
                    .first(doc)
                    .map(|dt| SortKeyValue::I64(crate::convert::millis_from_datetime(dt)))
                    .unwrap_or(SortKeyValue::None),
                KeyExtractor::Str(s) => match s.ords().first(doc) {
                    Some(ord) => {
                        let mut buf = String::new();
                        match s.ord_to_str(ord, &mut buf) {
                            Ok(true) => SortKeyValue::Str(buf),
                            _ => SortKeyValue::None,
                        }
                    }
                    None => SortKeyValue::None,
                },
            });
        }
        self.matches
            .push((key, DocAddress::new(self.segment_ord, doc), score));
    }

    fn harvest(self) -> Self::Fruit {
        // Per-segment top-N reduces the fruit shipped to merge_fruits. Sort globally there too.
        let mut matches = self.matches;
        matches.sort_by(|a, b| cmp_keys(&a.0, &b.0, &self.ascending));
        matches.truncate(self.limit);
        matches
    }
}
