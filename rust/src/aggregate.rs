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
        pb::AggregationType::AggDateHistogram => {
            let interval = f.date_fixed_interval.as_deref().ok_or_else(|| {
                ScantivyError::UnsupportedQuery(format!(
                    "date_histogram '{}' requires `date_fixed_interval`",
                    f.alias
                ))
            })?;
            let mut body = Map::new();
            body.insert("field".into(), J::String(f.field.clone()));
            body.insert("fixed_interval".into(), J::String(interval.to_string()));
            if let Some(min) = f.date_min_doc_count {
                body.insert("min_doc_count".into(), J::from(min));
            }
            if let Some(off) = f.date_offset.as_deref() {
                body.insert("offset".into(), J::String(off.to_string()));
            }
            json!({ "date_histogram": J::Object(body) })
        }
        pb::AggregationType::AggRange => {
            if f.range_buckets.is_empty() {
                return Err(ScantivyError::UnsupportedQuery(format!(
                    "range '{}' requires at least one bucket",
                    f.alias
                )));
            }
            let ranges: Vec<J> = f
                .range_buckets
                .iter()
                .map(|r| {
                    let mut o = Map::new();
                    if let Some(k) = &r.key {
                        o.insert("key".into(), J::String(k.clone()));
                    }
                    if let Some(from) = r.from {
                        o.insert("from".into(), J::from(from));
                    }
                    if let Some(to) = r.to {
                        o.insert("to".into(), J::from(to));
                    }
                    J::Object(o)
                })
                .collect();
            json!({ "range": { "field": f.field, "ranges": ranges } })
        }
        pb::AggregationType::AggPercentiles => {
            let mut body = Map::new();
            body.insert("field".into(), J::String(f.field.clone()));
            if !f.percentiles.is_empty() {
                body.insert(
                    "percents".into(),
                    J::Array(f.percentiles.iter().map(|p| J::from(*p)).collect()),
                );
            }
            json!({ "percentiles": J::Object(body) })
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

    // Attach sub-aggregations under the standard `aggs` key, alongside the bucket-type body. Only
    // bucket aggs (terms / histogram / date_histogram / range) accept sub-aggs; non-buckets
    // are rejected with a clear error rather than silently ignored.
    if !f.sub_aggregations.is_empty() {
        if !is_bucket_kind(ty) {
            return Err(ScantivyError::UnsupportedQuery(format!(
                "sub_aggregations on '{}' are only supported for bucket aggregations \
                 (terms, histogram, date_histogram, range)",
                f.alias
            )));
        }
        for sub in &f.sub_aggregations {
            let sub_ty = pb::AggregationType::try_from(sub.r#type)
                .unwrap_or(pb::AggregationType::AggUnspecified);
            if sub_ty == pb::AggregationType::AggConcat {
                return Err(ScantivyError::UnsupportedQuery(
                    "concat is not supported as a sub-aggregation (custom path)".into(),
                ));
            }
        }
        let mut combined = body
            .as_object()
            .cloned()
            .expect("agg body is always a JSON object");
        let mut subs = Map::new();
        for sub in &f.sub_aggregations {
            subs.insert(sub.alias.clone(), build_agg_json(sub)?);
        }
        combined.insert("aggs".into(), J::Object(subs));
        return Ok(J::Object(combined));
    }
    Ok(body)
}

fn is_bucket_kind(ty: pb::AggregationType) -> bool {
    matches!(
        ty,
        pb::AggregationType::AggTerms
            | pb::AggregationType::AggHistogram
            | pb::AggregationType::AggDateHistogram
            | pb::AggregationType::AggRange
    )
}

fn decode_sub_results(f: &pb::AggregationFunction, bucket_json: &J) -> Vec<pb::AggregationResult> {
    f.sub_aggregations
        .iter()
        .map(|sub| decode_result(sub, bucket_json))
        .collect()
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
                                sub_results: decode_sub_results(f, b),
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
                            pb::HistogramBucket {
                                key,
                                doc_count,
                                sub_results: decode_sub_results(f, b),
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();
            Some(pb::aggregation_result::Value::Histogram(
                pb::HistogramBucketList { buckets },
            ))
        }
        pb::AggregationType::AggDateHistogram => {
            let buckets = e
                .get("buckets")
                .and_then(|b| b.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|b| {
                            // Tantivy emits `key` in millis as f64; round to i64 for proto.
                            let key_millis =
                                b.get("key").and_then(|n| n.as_f64()).unwrap_or(0.0) as i64;
                            let key_as_string = b
                                .get("key_as_string")
                                .and_then(|s| s.as_str())
                                .map(str::to_string);
                            let doc_count =
                                b.get("doc_count").and_then(|n| n.as_u64()).unwrap_or(0);
                            pb::DateHistogramBucket {
                                key_millis,
                                doc_count,
                                key_as_string,
                                sub_results: decode_sub_results(f, b),
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();
            Some(pb::aggregation_result::Value::DateHistogram(
                pb::DateHistogramBucketList { buckets },
            ))
        }
        pb::AggregationType::AggRange => {
            let buckets = e
                .get("buckets")
                .and_then(|b| b.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|b| {
                            let key = b.get("key").and_then(|s| s.as_str()).map(str::to_string);
                            let from = b.get("from").and_then(|n| n.as_f64());
                            let to = b.get("to").and_then(|n| n.as_f64());
                            let doc_count =
                                b.get("doc_count").and_then(|n| n.as_u64()).unwrap_or(0);
                            pb::RangeBucket {
                                key,
                                from,
                                to,
                                doc_count,
                                sub_results: decode_sub_results(f, b),
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();
            Some(pb::aggregation_result::Value::Range(pb::RangeBucketList {
                buckets,
            }))
        }
        pb::AggregationType::AggPercentiles => {
            // Tantivy emits either `{"values": {"50.0": 12.3, "95.0": 17.1}}` (default keyed=true)
            // or `{"values": [{"key": 50.0, "value": 12.3}, ...]}` (keyed=false). We don't expose
            // `keyed` in the proto, so accept either shape here.
            let mut values: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            if let Some(map) = e.get("values").and_then(|v| v.as_object()) {
                for (k, v) in map {
                    if let Some(n) = v.as_f64() {
                        values.insert(k.clone(), n);
                    }
                }
            } else if let Some(arr) = e.get("values").and_then(|v| v.as_array()) {
                for entry in arr {
                    let k = entry.get("key").and_then(|x| x.as_f64());
                    let v = entry.get("value").and_then(|x| x.as_f64());
                    if let (Some(k), Some(v)) = (k, v) {
                        values.insert(format!("{}", k), v);
                    }
                }
            }
            Some(pb::aggregation_result::Value::Percentiles(
                pb::PercentilesResult { values },
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
