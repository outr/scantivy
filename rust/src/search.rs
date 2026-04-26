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
use tantivy::query::{AllQuery, Query};
use tantivy::schema::FieldType;
use tantivy::{DocAddress, DocId, Order, Score, SegmentOrdinal, SegmentReader, TantivyDocument};

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

    let sort = req.sort.first().and_then(|s| s.clause.clone());

    // Run the top-N search; for sort-by-fast-field this produces (Option<...>, DocAddress) so we
    // discard the field value and treat score as 0 for downstream uniformity.
    let scored: Vec<(Score, DocAddress)> = match &sort {
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
        hits.push(pb::SearchHit {
            payload: Some(payload),
            score: if req.score_docs { Some(score) } else { None },
            id: id_value,
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
