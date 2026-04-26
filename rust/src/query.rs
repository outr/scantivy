//! Proto Query → Tantivy `Box<dyn Query>` compilation.

use crate::convert::value_to_term;
use crate::error::{Result, ScantivyError};
use crate::pb;
use crate::schema::ResolvedSchema;
use regex::escape as regex_escape;
use std::ops::Bound;
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, EmptyQuery, Occur, PhraseQuery, Query, QueryParser,
    RangeQuery, RegexQuery, TermQuery, TermSetQuery,
};
use tantivy::schema::{Facet, FieldType, IndexRecordOption};
use tantivy::{Index, Term};

pub fn compile(
    index: &Index,
    schema: &ResolvedSchema,
    proto: &pb::Query,
) -> Result<Box<dyn Query>> {
    let node = proto
        .node
        .as_ref()
        .ok_or_else(|| ScantivyError::UnsupportedQuery("empty Query node".into()))?;
    Ok(match node {
        pb::query::Node::All(_) => Box::new(AllQuery),
        pb::query::Node::None(_) => Box::new(EmptyQuery),
        pb::query::Node::Term(t) => compile_term(schema, t)?,
        pb::query::Node::Range(r) => compile_range(schema, r)?,
        pb::query::Node::InSet(in_q) => compile_in(schema, in_q)?,
        // Tantivy's RegexQuery matches the *entire* field value and rejects the `^`/`$` anchor
        // operators (it calls them "empty match operators"). So we model startsWith/endsWith/
        // contains by anchoring with `.*` instead of `^`/`$`.
        pb::query::Node::StartsWith(p) => {
            compile_regex(schema, &p.field, format!("{}.*", regex_escape(&p.value)))?
        }
        pb::query::Node::EndsWith(p) => {
            compile_regex(schema, &p.field, format!(".*{}", regex_escape(&p.value)))?
        }
        pb::query::Node::Contains(p) => {
            compile_regex(schema, &p.field, format!(".*{}.*", regex_escape(&p.value)))?
        }
        pb::query::Node::Regex(p) => compile_regex(schema, &p.field, p.pattern.clone())?,
        pb::query::Node::Exact(e) => compile_exact(schema, e)?,
        pb::query::Node::FullText(f) => compile_full_text(index, schema, f)?,
        pb::query::Node::Phrase(p) => compile_phrase(schema, p)?,
        pb::query::Node::DrillDown(d) => compile_drill_down(schema, d)?,
        pb::query::Node::BoolQuery(b) => compile_bool(index, schema, b)?,
    })
}

fn compile_term(schema: &ResolvedSchema, t: &pb::QueryTerm) -> Result<Box<dyn Query>> {
    let value = t.value.as_ref().ok_or_else(|| {
        ScantivyError::UnsupportedQuery(format!("term query missing value for field '{}'", t.field))
    })?;
    let term = value_to_term(schema, &t.field, value)?;
    Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
}

fn compile_range(schema: &ResolvedSchema, r: &pb::QueryRange) -> Result<Box<dyn Query>> {
    let field = schema.field(&r.field)?;
    let entry = schema.schema.get_field_entry(field);

    fn bound(
        v: Option<&pb::Value>,
        excluded: bool,
        f: &dyn Fn(&pb::Value) -> Option<Term>,
    ) -> Bound<Term> {
        match v.and_then(f) {
            Some(t) if excluded => Bound::Excluded(t),
            Some(t) => Bound::Included(t),
            None => Bound::Unbounded,
        }
    }

    type TermFn<'a> = Box<dyn Fn(&pb::Value) -> Option<Term> + 'a>;
    let mk_term: TermFn<'_> = match entry.field_type() {
        FieldType::I64(_) => Box::new(move |v| match v.kind.as_ref()? {
            pb::value::Kind::LongValue(n) => Some(Term::from_field_i64(field, *n)),
            _ => None,
        }),
        FieldType::U64(_) => Box::new(move |v| match v.kind.as_ref()? {
            pb::value::Kind::UlongValue(n) => Some(Term::from_field_u64(field, *n)),
            _ => None,
        }),
        FieldType::F64(_) => Box::new(move |v| match v.kind.as_ref()? {
            pb::value::Kind::DoubleValue(n) => Some(Term::from_field_f64(field, *n)),
            _ => None,
        }),
        FieldType::Date(_) => Box::new(move |v| match v.kind.as_ref()? {
            pb::value::Kind::DateMillis(ms) => Some(Term::from_field_date(
                field,
                crate::convert::datetime_from_millis(*ms),
            )),
            _ => None,
        }),
        FieldType::Bytes(_) => Box::new(move |v| match v.kind.as_ref()? {
            pb::value::Kind::BytesValue(b) => Some(Term::from_field_bytes(field, b.as_slice())),
            _ => None,
        }),
        FieldType::Str(_) => Box::new(move |v| match v.kind.as_ref()? {
            pb::value::Kind::StringValue(s) => Some(Term::from_field_text(field, s)),
            _ => None,
        }),
        ft => {
            return Err(ScantivyError::UnsupportedFieldType(format!(
                "range not supported for field type {:?}",
                ft
            )));
        }
    };

    let lower = if r.gt.is_some() {
        bound(r.gt.as_ref(), true, mk_term.as_ref())
    } else {
        bound(r.gte.as_ref(), false, mk_term.as_ref())
    };
    let upper = if r.lt.is_some() {
        bound(r.lt.as_ref(), true, mk_term.as_ref())
    } else {
        bound(r.lte.as_ref(), false, mk_term.as_ref())
    };

    Ok(Box::new(RangeQuery::new(lower, upper)))
}

fn compile_in(schema: &ResolvedSchema, q: &pb::QueryIn) -> Result<Box<dyn Query>> {
    let mut terms = Vec::with_capacity(q.values.len());
    for v in &q.values {
        terms.push(value_to_term(schema, &q.field, v)?);
    }
    Ok(Box::new(TermSetQuery::new(terms)))
}

fn compile_regex(
    schema: &ResolvedSchema,
    field_name: &str,
    pattern: String,
) -> Result<Box<dyn Query>> {
    let field = schema.field(field_name)?;
    Ok(Box::new(RegexQuery::from_pattern(&pattern, field)?))
}

fn compile_exact(schema: &ResolvedSchema, e: &pb::QueryExact) -> Result<Box<dyn Query>> {
    let field = schema.field(&e.field)?;
    let term = Term::from_field_text(field, &e.value);
    Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
}

fn compile_full_text(
    index: &Index,
    schema: &ResolvedSchema,
    f: &pb::QueryFullText,
) -> Result<Box<dyn Query>> {
    let fields = if f.fields.is_empty() {
        schema.text_fields.clone()
    } else {
        let mut out = Vec::with_capacity(f.fields.len());
        for n in &f.fields {
            out.push(schema.field(n)?);
        }
        out
    };
    if fields.is_empty() {
        return Err(ScantivyError::UnsupportedQuery(
            "QueryFullText requires at least one TEXT field".into(),
        ));
    }
    let parser = QueryParser::for_index(index, fields);
    Ok(parser.parse_query(&f.query)?)
}

fn compile_phrase(schema: &ResolvedSchema, p: &pb::QueryPhrase) -> Result<Box<dyn Query>> {
    let field = schema.field(&p.field)?;
    let terms: Vec<Term> = p
        .terms
        .iter()
        .map(|t| Term::from_field_text(field, t))
        .collect();
    if terms.is_empty() {
        return Ok(Box::new(EmptyQuery));
    }
    let phrase = PhraseQuery::new(terms);
    if p.slop > 0 {
        let mut q = phrase;
        q.set_slop(p.slop);
        Ok(Box::new(q))
    } else {
        Ok(Box::new(phrase))
    }
}

fn compile_drill_down(schema: &ResolvedSchema, d: &pb::QueryDrillDown) -> Result<Box<dyn Query>> {
    let field = schema.field(&d.field)?;
    let mut path: Vec<&str> = d.path.iter().map(|s| s.as_str()).collect();
    let _ = d.only_this_level; // not honored without DrillSideways; logged by caller
    let facet = if path.is_empty() {
        Facet::root()
    } else {
        Facet::from_path(path.drain(..))
    };
    Ok(Box::new(TermQuery::new(
        Term::from_facet(field, &facet),
        IndexRecordOption::Basic,
    )))
}

fn compile_bool(
    index: &Index,
    schema: &ResolvedSchema,
    b: &pb::QueryBool,
) -> Result<Box<dyn Query>> {
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    for q in &b.must {
        clauses.push((Occur::Must, compile(index, schema, q)?));
    }
    for q in &b.should {
        clauses.push((Occur::Should, compile(index, schema, q)?));
    }
    for q in &b.must_not {
        clauses.push((Occur::MustNot, compile(index, schema, q)?));
    }
    for q in &b.filter {
        clauses.push((Occur::Must, compile(index, schema, q)?));
    }
    if clauses.is_empty() {
        return Ok(Box::new(AllQuery));
    }
    let mut bq = BooleanQuery::new(clauses);
    if let Some(min) = b.min_should_match {
        bq.set_minimum_number_should_match(min as usize);
    }
    Ok(Box::new(bq))
}

#[allow(dead_code)]
fn _unused() {
    let _ = BoostQuery::new(Box::new(AllQuery), 1.0); // keep import live for future scoring tweaks
}
