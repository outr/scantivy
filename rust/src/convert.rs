//! Proto Value ↔ Tantivy term/owned-value/document conversion.

use crate::error::{Result, ScantivyError};
use crate::pb;
use crate::schema::ResolvedSchema;
use std::str::FromStr;
use tantivy::schema::{Facet, FieldType, OwnedValue, Value as TantivyValueTrait};
use tantivy::time::OffsetDateTime;
use tantivy::{DateTime, TantivyDocument, Term};

/// Convert proto Value into a Tantivy Term for the given field.
pub fn value_to_term(schema: &ResolvedSchema, field_name: &str, value: &pb::Value) -> Result<Term> {
    let field = schema.field(field_name)?;
    let entry = schema.schema.get_field_entry(field);
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| ScantivyError::InvalidValue {
            field: field_name.to_string(),
            reason: "no value kind set".into(),
        })?;
    let term = match (entry.field_type(), kind) {
        (FieldType::Str(_), pb::value::Kind::StringValue(s)) => Term::from_field_text(field, s),
        (FieldType::I64(_), pb::value::Kind::LongValue(v)) => Term::from_field_i64(field, *v),
        (FieldType::U64(_), pb::value::Kind::UlongValue(v)) => Term::from_field_u64(field, *v),
        (FieldType::F64(_), pb::value::Kind::DoubleValue(v)) => Term::from_field_f64(field, *v),
        (FieldType::Bool(_), pb::value::Kind::BoolValue(v)) => Term::from_field_bool(field, *v),
        (FieldType::Date(_), pb::value::Kind::DateMillis(ms)) => {
            Term::from_field_date(field, datetime_from_millis(*ms))
        }
        (FieldType::Bytes(_), pb::value::Kind::BytesValue(b)) => {
            Term::from_field_bytes(field, b.as_slice())
        }
        (FieldType::Facet(_), pb::value::Kind::FacetValue(s)) => {
            Term::from_facet(field, &Facet::from(s.as_str()))
        }
        (FieldType::IpAddr(_), pb::value::Kind::IpValue(s)) => {
            let ip = std::net::IpAddr::from_str(s).map_err(|_| ScantivyError::InvalidValue {
                field: field_name.to_string(),
                reason: format!("invalid ip address '{}'", s),
            })?;
            let v6 = match ip {
                std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                std::net::IpAddr::V6(v6) => v6,
            };
            Term::from_field_ip_addr(field, v6)
        }
        (ft, _) => {
            return Err(ScantivyError::UnsupportedFieldType(format!(
                "term not supported for field '{}' kind={:?}",
                field_name, ft
            )));
        }
    };
    Ok(term)
}

pub fn datetime_from_millis(ms: i64) -> DateTime {
    let secs = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) as u32) * 1_000_000;
    DateTime::from_utc(
        OffsetDateTime::from_unix_timestamp_nanos(secs as i128 * 1_000_000_000 + nanos as i128)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    )
}

pub fn millis_from_datetime(dt: DateTime) -> i64 {
    let nanos = dt.into_utc().unix_timestamp_nanos();
    (nanos / 1_000_000) as i64
}

/// Append all values from a proto FieldValue to the Tantivy document.
pub fn append_field_value(
    schema: &ResolvedSchema,
    doc: &mut TantivyDocument,
    fv: &pb::FieldValue,
) -> Result<()> {
    let field = schema.field(&fv.name)?;
    let entry = schema.schema.get_field_entry(field);
    for val in &fv.values {
        let kind = val
            .kind
            .as_ref()
            .ok_or_else(|| ScantivyError::InvalidValue {
                field: fv.name.clone(),
                reason: "no value kind set".into(),
            })?;
        match (entry.field_type(), kind) {
            (FieldType::Str(_), pb::value::Kind::StringValue(s)) => doc.add_text(field, s),
            (FieldType::I64(_), pb::value::Kind::LongValue(v)) => doc.add_i64(field, *v),
            (FieldType::U64(_), pb::value::Kind::UlongValue(v)) => doc.add_u64(field, *v),
            (FieldType::F64(_), pb::value::Kind::DoubleValue(v)) => doc.add_f64(field, *v),
            (FieldType::Bool(_), pb::value::Kind::BoolValue(v)) => doc.add_bool(field, *v),
            (FieldType::Date(_), pb::value::Kind::DateMillis(ms)) => {
                doc.add_date(field, datetime_from_millis(*ms))
            }
            (FieldType::Bytes(_), pb::value::Kind::BytesValue(b)) => {
                doc.add_bytes(field, b.as_slice())
            }
            (FieldType::Facet(_), pb::value::Kind::FacetValue(s)) => {
                doc.add_facet(field, Facet::from(s.as_str()))
            }
            (FieldType::IpAddr(_), pb::value::Kind::IpValue(s)) => {
                let ip =
                    std::net::IpAddr::from_str(s).map_err(|_| ScantivyError::InvalidValue {
                        field: fv.name.clone(),
                        reason: format!("invalid ip '{}'", s),
                    })?;
                let v6 = match ip {
                    std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                    std::net::IpAddr::V6(v6) => v6,
                };
                doc.add_ip_addr(field, v6)
            }
            (FieldType::JsonObject(_), pb::value::Kind::JsonValue(jv)) => {
                let parsed: serde_json::Value =
                    serde_json::from_slice(&jv.encoded).map_err(|e| {
                        ScantivyError::InvalidValue {
                            field: fv.name.clone(),
                            reason: format!("invalid JSON: {}", e),
                        }
                    })?;
                let map =
                    parsed
                        .as_object()
                        .cloned()
                        .ok_or_else(|| ScantivyError::InvalidValue {
                            field: fv.name.clone(),
                            reason: "JSON value must be an object".into(),
                        })?;
                let owned: std::collections::BTreeMap<String, OwnedValue> = map
                    .into_iter()
                    .map(|(k, v)| (k, json_to_owned(v)))
                    .collect();
                doc.add_field_value(field, &OwnedValue::Object(owned.into_iter().collect()));
            }
            (ft, _) => {
                return Err(ScantivyError::UnsupportedFieldType(format!(
                    "doc value not supported for field '{}' kind={:?}",
                    fv.name, ft
                )));
            }
        }
    }
    Ok(())
}

fn json_to_owned(v: serde_json::Value) -> OwnedValue {
    match v {
        serde_json::Value::Null => OwnedValue::Null,
        serde_json::Value::Bool(b) => OwnedValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                OwnedValue::I64(i)
            } else if let Some(u) = n.as_u64() {
                OwnedValue::U64(u)
            } else {
                OwnedValue::F64(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => OwnedValue::Str(s),
        serde_json::Value::Array(arr) => {
            OwnedValue::Array(arr.into_iter().map(json_to_owned).collect())
        }
        serde_json::Value::Object(map) => OwnedValue::Object(
            map.into_iter()
                .map(|(k, v)| (k, json_to_owned(v)))
                .collect(),
        ),
    }
}

pub fn document_to_proto(schema: &ResolvedSchema, doc: &TantivyDocument) -> Result<pb::Document> {
    use std::collections::BTreeMap;
    let mut grouped: BTreeMap<u32, Vec<pb::Value>> = BTreeMap::new();
    for (field, raw) in doc.field_values() {
        let owned: OwnedValue = TantivyValueTrait::as_value(&raw).into();
        let entry = schema.schema.get_field_entry(field);
        let pv = owned_to_proto(entry.field_type(), &owned)?;
        grouped.entry(field.field_id()).or_default().push(pv);
    }
    let fields: Vec<pb::FieldValue> = grouped
        .into_iter()
        .map(|(field_id, values)| {
            let entry = schema
                .schema
                .get_field_entry(Field::from_field_id(field_id));
            pb::FieldValue {
                name: entry.name().to_string(),
                values,
            }
        })
        .collect();
    Ok(pb::Document { fields })
}

use tantivy::schema::Field;

fn owned_to_proto(ft: &FieldType, v: &OwnedValue) -> Result<pb::Value> {
    let kind = match (ft, v) {
        (FieldType::Str(_), OwnedValue::Str(s)) => pb::value::Kind::StringValue(s.clone()),
        (FieldType::I64(_), OwnedValue::I64(n)) => pb::value::Kind::LongValue(*n),
        (FieldType::U64(_), OwnedValue::U64(n)) => pb::value::Kind::UlongValue(*n),
        (FieldType::F64(_), OwnedValue::F64(n)) => pb::value::Kind::DoubleValue(*n),
        (FieldType::Bool(_), OwnedValue::Bool(b)) => pb::value::Kind::BoolValue(*b),
        (FieldType::Date(_), OwnedValue::Date(dt)) => {
            pb::value::Kind::DateMillis(millis_from_datetime(*dt))
        }
        (FieldType::Bytes(_), OwnedValue::Bytes(b)) => pb::value::Kind::BytesValue(b.clone()),
        (FieldType::Facet(_), OwnedValue::Facet(f)) => {
            pb::value::Kind::FacetValue(f.to_path_string())
        }
        (FieldType::IpAddr(_), OwnedValue::IpAddr(ip)) => {
            let s = if let Some(v4) = ip.to_ipv4_mapped() {
                v4.to_string()
            } else {
                ip.to_string()
            };
            pb::value::Kind::IpValue(s)
        }
        (FieldType::JsonObject(_), OwnedValue::Object(_)) => {
            let json = owned_to_json(v);
            let bytes = serde_json::to_vec(&json).map_err(|e| ScantivyError::InvalidValue {
                field: "<json>".into(),
                reason: e.to_string(),
            })?;
            pb::value::Kind::JsonValue(pb::JsonValue { encoded: bytes })
        }
        (ft, ov) => {
            return Err(ScantivyError::UnsupportedFieldType(format!(
                "stored value not supported for field type {:?} value {:?}",
                ft, ov
            )));
        }
    };
    Ok(pb::Value { kind: Some(kind) })
}

fn owned_to_json(v: &OwnedValue) -> serde_json::Value {
    match v {
        OwnedValue::Null => serde_json::Value::Null,
        OwnedValue::Bool(b) => serde_json::Value::Bool(*b),
        OwnedValue::I64(n) => serde_json::Value::from(*n),
        OwnedValue::U64(n) => serde_json::Value::from(*n),
        OwnedValue::F64(n) => serde_json::Value::from(*n),
        OwnedValue::Str(s) => serde_json::Value::String(s.clone()),
        OwnedValue::Array(arr) => serde_json::Value::Array(arr.iter().map(owned_to_json).collect()),
        OwnedValue::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), owned_to_json(v)))
                .collect(),
        ),
        OwnedValue::Date(dt) => serde_json::Value::from(millis_from_datetime(*dt)),
        OwnedValue::Facet(f) => serde_json::Value::String(f.to_path_string()),
        OwnedValue::Bytes(b) => serde_json::Value::String(hex::encode(b)),
        OwnedValue::IpAddr(ip) => serde_json::Value::String(ip.to_string()),
        OwnedValue::PreTokStr(p) => serde_json::Value::String(p.text.clone()),
    }
}
