//! Aggregations via `tantivy::aggregation::*` plus a small custom path for `Concat`.

use crate::error::{Result, ScantivyError};
use crate::index::IndexHandle;
use crate::pb;
use crate::query::compile;
use serde_json::{Map, Value as J, json};
use tantivy::TantivyDocument;
use tantivy::aggregation::AggregationCollector;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::collector::TopDocs;
use tantivy::query::AllQuery;
use tantivy::schema::{FieldType, OwnedValue, Value as TantivyValueTrait};

const DEFAULT_CONCAT_MAX: u32 = 1000;
const DEFAULT_CONCAT_SEP: &str = ", ";

pub fn aggregate(
    handle: &IndexHandle,
    req: &pb::AggregationRequest,
) -> Result<pb::AggregationResponse> {
    let q = match req.filter_query.as_ref() {
        Some(q) => compile(&handle.index, &handle.schema, q)?,
        None => Box::new(AllQuery) as Box<dyn tantivy::query::Query>,
    };

    // Tantivy aggs handle every function except Concat. Concat is a separate top-N + store walk.
    let (concat_fns, builtin_fns): (Vec<_>, Vec<_>) = req.functions.iter().partition(|f| {
        matches!(
            pb::AggregationType::try_from(f.r#type),
            Ok(pb::AggregationType::AggConcat)
        )
    });

    handle.reader.reload().ok();
    let searcher = handle.reader.searcher();

    let res_json: J = if builtin_fns.is_empty() {
        J::Null
    } else {
        let mut agg_obj = Map::new();
        for f in &builtin_fns {
            agg_obj.insert(f.alias.clone(), build_agg_json(f)?);
        }
        let aggs: Aggregations = serde_json::from_value(J::Object(agg_obj)).map_err(|e| {
            ScantivyError::UnsupportedQuery(format!("invalid aggregation request: {}", e))
        })?;
        let collector = AggregationCollector::from_aggs(aggs, Default::default());
        let agg_res: AggregationResults = searcher.search(&q, &collector)?;
        serde_json::to_value(&agg_res).unwrap_or(J::Null)
    };

    let mut concat_results: std::collections::HashMap<String, String> =
        std::collections::HashMap::with_capacity(concat_fns.len());
    if !concat_fns.is_empty() {
        // One TopDocs pass shared across all concat aggs (they all see the same matched doc set).
        let max_docs = concat_fns
            .iter()
            .map(|f| f.concat_max_values.unwrap_or(DEFAULT_CONCAT_MAX) as usize)
            .max()
            .unwrap_or(DEFAULT_CONCAT_MAX as usize);
        let top: Vec<(tantivy::Score, tantivy::DocAddress)> =
            searcher.search(&q, &TopDocs::with_limit(max_docs.max(1)).order_by_score())?;
        for f in &concat_fns {
            let cap = f.concat_max_values.unwrap_or(DEFAULT_CONCAT_MAX) as usize;
            let sep = f.concat_separator.as_deref().unwrap_or(DEFAULT_CONCAT_SEP);
            let joined = concat_field(handle, &searcher, &top, &f.field, cap, sep)?;
            concat_results.insert(f.alias.clone(), joined);
        }
    }

    let results: Vec<pb::AggregationResult> = req
        .functions
        .iter()
        .map(|f| {
            let ty = pb::AggregationType::try_from(f.r#type)
                .unwrap_or(pb::AggregationType::AggUnspecified);
            if ty == pb::AggregationType::AggConcat {
                pb::AggregationResult {
                    alias: f.alias.clone(),
                    value: concat_results
                        .remove(&f.alias)
                        .map(pb::aggregation_result::Value::Concat),
                }
            } else {
                decode_result(f, &res_json)
            }
        })
        .collect();

    Ok(pb::AggregationResponse {
        results,
        error: None,
    })
}

fn concat_field(
    handle: &IndexHandle,
    searcher: &tantivy::Searcher,
    top: &[(tantivy::Score, tantivy::DocAddress)],
    field_name: &str,
    cap: usize,
    sep: &str,
) -> Result<String> {
    let field = handle.schema.field(field_name)?;
    let entry = handle.schema.schema.get_field_entry(field);
    if !entry.is_stored() {
        return Err(ScantivyError::UnsupportedFieldType(format!(
            "concat requires field '{}' to be stored",
            field_name
        )));
    }
    let mut parts: Vec<String> = Vec::new();
    for (_score, addr) in top.iter().take(cap) {
        let doc: TantivyDocument = searcher.doc(*addr)?;
        for (f, raw) in doc.field_values() {
            if f != field {
                continue;
            }
            let owned: OwnedValue = TantivyValueTrait::as_value(&raw).into();
            if let Some(s) = owned_to_concat_string(entry.field_type(), &owned) {
                parts.push(s);
            }
        }
    }
    Ok(parts.join(sep))
}

fn owned_to_concat_string(ft: &FieldType, v: &OwnedValue) -> Option<String> {
    match (ft, v) {
        (FieldType::Str(_), OwnedValue::Str(s)) => Some(s.clone()),
        (FieldType::I64(_), OwnedValue::I64(n)) => Some(n.to_string()),
        (FieldType::U64(_), OwnedValue::U64(n)) => Some(n.to_string()),
        (FieldType::F64(_), OwnedValue::F64(n)) => Some(n.to_string()),
        (FieldType::Bool(_), OwnedValue::Bool(b)) => Some(b.to_string()),
        (FieldType::Facet(_), OwnedValue::Facet(f)) => Some(f.to_path_string()),
        (FieldType::IpAddr(_), OwnedValue::IpAddr(ip)) => Some(ip.to_string()),
        _ => None,
    }
}

fn build_agg_json(f: &pb::AggregationFunction) -> Result<J> {
    let ty = pb::AggregationType::try_from(f.r#type).unwrap_or(pb::AggregationType::AggUnspecified);
    let body = match ty {
        pb::AggregationType::AggSum => json!({"sum": {"field": f.field}}),
        pb::AggregationType::AggAvg => json!({"avg": {"field": f.field}}),
        pb::AggregationType::AggMin => json!({"min": {"field": f.field}}),
        pb::AggregationType::AggMax => json!({"max": {"field": f.field}}),
        pb::AggregationType::AggCount => json!({"value_count": {"field": f.field}}),
        pb::AggregationType::AggStats => json!({"stats": {"field": f.field}}),
        pb::AggregationType::AggCardinality => {
            json!({"cardinality": {"field": f.field}})
        }
        pb::AggregationType::AggHistogram => {
            let interval = f.histogram_interval.unwrap_or(1.0);
            json!({"histogram": {"field": f.field, "interval": interval}})
        }
        pb::AggregationType::AggTerms => {
            let size = f.terms_size.unwrap_or(10);
            json!({"terms": {"field": f.field, "size": size}})
        }
        pb::AggregationType::AggConcat => {
            // Routed through the custom concat path; should never reach build_agg_json.
            return Err(ScantivyError::UnsupportedQuery(
                "internal: concat aggregation should be handled separately".into(),
            ));
        }
        pb::AggregationType::AggUnspecified => {
            return Err(ScantivyError::UnsupportedQuery(format!(
                "unspecified aggregation type for alias '{}'",
                f.alias
            )));
        }
    };
    Ok(body)
}

fn decode_result(f: &pb::AggregationFunction, res: &J) -> pb::AggregationResult {
    let entry = res.get(&f.alias);
    let ty = pb::AggregationType::try_from(f.r#type).unwrap_or(pb::AggregationType::AggUnspecified);
    let value = entry.and_then(|e| match ty {
        pb::AggregationType::AggSum
        | pb::AggregationType::AggAvg
        | pb::AggregationType::AggMin
        | pb::AggregationType::AggMax
        | pb::AggregationType::AggCount
        | pb::AggregationType::AggCardinality => {
            let v = e.get("value").and_then(|x| x.as_f64()).unwrap_or(0.0);
            Some(pb::aggregation_result::Value::Numeric(v))
        }
        pb::AggregationType::AggStats => {
            let count = e.get("count").and_then(|x| x.as_u64()).unwrap_or(0);
            let sum = e.get("sum").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let min = e.get("min").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let max = e.get("max").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let avg = e.get("avg").and_then(|x| x.as_f64()).unwrap_or(0.0);
            Some(pb::aggregation_result::Value::Stats(pb::StatsResult {
                count,
                sum,
                min,
                max,
                avg,
            }))
        }
        pb::AggregationType::AggTerms => {
            let buckets = e
                .get("buckets")
                .and_then(|b| b.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|b| {
                            let key = b
                                .get("key")
                                .map(json_to_value)
                                .unwrap_or(pb::Value { kind: None });
                            let doc_count =
                                b.get("doc_count").and_then(|n| n.as_u64()).unwrap_or(0);
                            pb::TermBucket {
                                key: Some(key),
                                doc_count,
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();
            Some(pb::aggregation_result::Value::Terms(pb::TermBucketList {
                buckets,
            }))
        }
        pb::AggregationType::AggHistogram => {
            let buckets = e
                .get("buckets")
                .and_then(|b| b.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|b| {
                            let key = b.get("key").and_then(|n| n.as_f64()).unwrap_or(0.0);
                            let doc_count =
                                b.get("doc_count").and_then(|n| n.as_u64()).unwrap_or(0);
                            pb::HistogramBucket { key, doc_count }
                        })
                        .collect()
                })
                .unwrap_or_default();
            Some(pb::aggregation_result::Value::Histogram(
                pb::HistogramBucketList { buckets },
            ))
        }
        pb::AggregationType::AggConcat | pb::AggregationType::AggUnspecified => None,
    });
    pb::AggregationResult {
        alias: f.alias.clone(),
        value,
    }
}

fn json_to_value(v: &J) -> pb::Value {
    let kind = match v {
        J::String(s) => Some(pb::value::Kind::StringValue(s.clone())),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(pb::value::Kind::LongValue(i))
            } else if let Some(u) = n.as_u64() {
                Some(pb::value::Kind::UlongValue(u))
            } else {
                Some(pb::value::Kind::DoubleValue(n.as_f64().unwrap_or(0.0)))
            }
        }
        J::Bool(b) => Some(pb::value::Kind::BoolValue(*b)),
        _ => None,
    };
    pb::Value { kind }
}
