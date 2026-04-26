//! Distinct streaming via terms aggregation with cursor paging.

use crate::error::{Result, ScantivyError};
use crate::index::IndexHandle;
use crate::pb;
use crate::query::compile;
use serde_json::{Value as J, json};
use tantivy::aggregation::AggregationCollector;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::query::AllQuery;

const ALIAS: &str = "_distinct";

pub fn distinct(handle: &IndexHandle, req: &pb::DistinctRequest) -> Result<pb::DistinctResponse> {
    let mut filter = match req.filter_query.as_ref() {
        Some(q) => compile(&handle.index, &handle.schema, q)?,
        None => Box::new(AllQuery) as Box<dyn tantivy::query::Query>,
    };

    // If a cursor is given, AND a "field > cursor" range filter onto the query so we resume.
    if let Some(cursor) = req.cursor.as_ref() {
        let extra = build_cursor_filter(handle, &req.field, cursor)?;
        filter = combine_must(filter, extra);
    }

    // Ask for exactly page_size entries. If we get a full page back, the cursor for the next page
    // is the last returned value (used in an exclusive range filter on the next call).
    let size = req.page_size.max(1);
    let agg_json = json!({
        ALIAS: {
            "terms": {
                "field": req.field,
                "size": size,
                "order": { "_key": "asc" },
                "min_doc_count": 1
            }
        }
    });
    let aggs: Aggregations = serde_json::from_value(agg_json).map_err(|e| {
        ScantivyError::UnsupportedQuery(format!("invalid distinct aggregation: {}", e))
    })?;

    handle.reader.reload().ok();
    let searcher = handle.reader.searcher();
    let collector = AggregationCollector::from_aggs(aggs, Default::default());
    let res: AggregationResults = searcher.search(&filter, &collector)?;
    let res_json = serde_json::to_value(&res).unwrap_or(J::Null);

    let buckets = res_json
        .get(ALIAS)
        .and_then(|v| v.get("buckets"))
        .and_then(|b| b.as_array())
        .cloned()
        .unwrap_or_default();

    let values: Vec<pb::Value> = buckets
        .iter()
        .filter_map(|b| b.get("key").map(json_to_value))
        .collect();

    // If we got a full page back, the next cursor is the last value (caller should re-query
    // with cursor=Some(last_value) to resume).
    let next_cursor = if values.len() == req.page_size as usize {
        values.last().cloned()
    } else {
        None
    };

    Ok(pb::DistinctResponse {
        values,
        next_cursor,
        error: None,
    })
}

fn build_cursor_filter(
    handle: &IndexHandle,
    field: &str,
    cursor: &pb::Value,
) -> Result<Box<dyn tantivy::query::Query>> {
    use std::ops::Bound;
    use tantivy::query::RangeQuery;
    let f = handle.schema.field(field)?;
    let entry = handle.schema.schema.get_field_entry(f);
    use tantivy::Term;
    use tantivy::schema::FieldType;
    let lower = match (entry.field_type(), cursor.kind.as_ref()) {
        (FieldType::Str(_), Some(pb::value::Kind::StringValue(s))) => Term::from_field_text(f, s),
        (FieldType::I64(_), Some(pb::value::Kind::LongValue(n))) => Term::from_field_i64(f, *n),
        (FieldType::U64(_), Some(pb::value::Kind::UlongValue(n))) => Term::from_field_u64(f, *n),
        (FieldType::F64(_), Some(pb::value::Kind::DoubleValue(n))) => Term::from_field_f64(f, *n),
        (FieldType::Date(_), Some(pb::value::Kind::DateMillis(ms))) => {
            Term::from_field_date(f, crate::convert::datetime_from_millis(*ms))
        }
        (ft, _) => {
            return Err(ScantivyError::UnsupportedFieldType(format!(
                "distinct cursor not supported for field '{}' kind={:?}",
                field, ft
            )));
        }
    };
    Ok(Box::new(RangeQuery::new(
        Bound::Excluded(lower),
        Bound::Unbounded,
    )))
}

fn combine_must(
    a: Box<dyn tantivy::query::Query>,
    b: Box<dyn tantivy::query::Query>,
) -> Box<dyn tantivy::query::Query> {
    use tantivy::query::{BooleanQuery, Occur};
    Box::new(BooleanQuery::new(vec![(Occur::Must, a), (Occur::Must, b)]))
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
