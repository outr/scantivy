//! Proto Query → Tantivy `Box<dyn Query>` compilation.

use crate::convert::value_to_term;
use crate::error::{Result, ScantivyError};
use crate::pb;
use crate::schema::ResolvedSchema;
use regex::escape as regex_escape;
use std::ops::Bound;
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, ConstScoreQuery, DisjunctionMaxQuery, EmptyQuery,
    ExistsQuery, FuzzyTermQuery, MoreLikeThisQuery, Occur, PhrasePrefixQuery, PhraseQuery, Query,
    QueryParser, RangeQuery, RegexQuery, TermQuery, TermSetQuery,
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
        pb::query::Node::Exists(e) => Box::new(ExistsQuery::new(e.field.clone(), false)),
        pb::query::Node::Fuzzy(f) => compile_fuzzy(schema, f)?,
        pb::query::Node::PhrasePrefix(p) => compile_phrase_prefix(schema, p)?,
        pb::query::Node::Boost(b) => {
            let inner = b
                .inner
                .as_ref()
                .ok_or_else(|| ScantivyError::UnsupportedQuery("boost: missing inner".into()))?;
            Box::new(BoostQuery::new(compile(index, schema, inner)?, b.factor))
        }
        pb::query::Node::DisjunctionMax(d) => compile_disjunction_max(index, schema, d)?,
        pb::query::Node::ConstScore(c) => {
            let inner = c.inner.as_ref().ok_or_else(|| {
                ScantivyError::UnsupportedQuery("const_score: missing inner".into())
            })?;
            Box::new(ConstScoreQuery::new(
                compile(index, schema, inner)?,
                c.score,
            ))
        }
        pb::query::Node::MoreLikeThis(m) => compile_more_like_this(index, schema, m)?,
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

fn compile_fuzzy(schema: &ResolvedSchema, f: &pb::QueryFuzzy) -> Result<Box<dyn Query>> {
    let field = schema.field(&f.field)?;
    let term = Term::from_field_text(field, &f.value);
    let distance = f.distance.min(255) as u8;
    Ok(if f.prefix {
        Box::new(FuzzyTermQuery::new_prefix(
            term,
            distance,
            f.transposition_cost_one,
        ))
    } else {
        Box::new(FuzzyTermQuery::new(
            term,
            distance,
            f.transposition_cost_one,
        ))
    })
}

fn compile_phrase_prefix(
    schema: &ResolvedSchema,
    p: &pb::QueryPhrasePrefix,
) -> Result<Box<dyn Query>> {
    let field = schema.field(&p.field)?;
    if p.terms.is_empty() {
        return Ok(Box::new(EmptyQuery));
    }
    let terms: Vec<Term> = p
        .terms
        .iter()
        .map(|t| Term::from_field_text(field, t))
        .collect();
    let mut q = PhrasePrefixQuery::new(terms);
    if let Some(max) = p.max_expansions {
        q.set_max_expansions(max);
    }
    Ok(Box::new(q))
}

fn compile_disjunction_max(
    index: &Index,
    schema: &ResolvedSchema,
    d: &pb::QueryDisjunctionMax,
) -> Result<Box<dyn Query>> {
    if d.disjuncts.is_empty() {
        return Ok(Box::new(EmptyQuery));
    }
    let mut compiled: Vec<Box<dyn Query>> = Vec::with_capacity(d.disjuncts.len());
    for q in &d.disjuncts {
        compiled.push(compile(index, schema, q)?);
    }
    Ok(match d.tie_breaker {
        Some(tb) => Box::new(DisjunctionMaxQuery::with_tie_breaker(compiled, tb)),
        None => Box::new(DisjunctionMaxQuery::new(compiled)),
    })
}

/// Resolve a doc by its id-field value, returning the first matching `DocAddress` or `None`.
pub fn lookup_doc_by_id(
    index: &Index,
    schema: &ResolvedSchema,
    id_field: &str,
    source_id: &str,
) -> Result<Option<tantivy::DocAddress>> {
    use tantivy::collector::TopDocs;
    let field = schema.field(id_field)?;
    let term = Term::from_field_text(field, source_id);
    let q = TermQuery::new(term, IndexRecordOption::Basic);
    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
        .try_into()?;
    let searcher = reader.searcher();
    let top: Vec<(f32, tantivy::DocAddress)> =
        searcher.search(&q, &TopDocs::with_limit(1).order_by_score())?;
    Ok(top.into_iter().next().map(|(_, addr)| addr))
}

fn compile_more_like_this(
    index: &Index,
    schema: &ResolvedSchema,
    m: &pb::QueryMoreLikeThis,
) -> Result<Box<dyn Query>> {
    let id_field = schema.id_field.as_deref().ok_or_else(|| {
        ScantivyError::UnsupportedQuery(
            "more_like_this requires the schema to declare an id_field".into(),
        )
    })?;
    let addr = lookup_doc_by_id(index, schema, id_field, &m.source_id)?.ok_or_else(|| {
        ScantivyError::UnsupportedQuery(format!(
            "more_like_this: no doc with {}='{}'",
            id_field, m.source_id
        ))
    })?;

    let mut builder = MoreLikeThisQuery::builder();
    if let Some(v) = m.min_doc_frequency {
        builder = builder.with_min_doc_frequency(v);
    }
    if let Some(v) = m.max_doc_frequency {
        builder = builder.with_max_doc_frequency(v);
    }
    if let Some(v) = m.min_term_frequency {
        builder = builder.with_min_term_frequency(v as usize);
    }
    if let Some(v) = m.max_query_terms {
        builder = builder.with_max_query_terms(v as usize);
    }
    if let Some(v) = m.min_word_length {
        builder = builder.with_min_word_length(v as usize);
    }
    if let Some(v) = m.max_word_length {
        builder = builder.with_max_word_length(v as usize);
    }
    if let Some(v) = m.boost_factor {
        builder = builder.with_boost_factor(v);
    }
    if !m.stop_words.is_empty() {
        builder = builder.with_stop_words(m.stop_words.clone());
    }
    let mlt: Box<dyn Query> = Box::new(builder.with_document(addr));

    // Tantivy's MoreLikeThis ranks the source doc itself at the top by default, which is rarely
    // what callers actually want. Wrap the MLT query in a BooleanQuery that excludes the source
    // by id-field term unless the caller explicitly opted in.
    let exclude = m.exclude_source.unwrap_or(true);
    if !exclude {
        return Ok(mlt);
    }
    let id_field_handle = schema.field(id_field)?;
    let exclude_term = TermQuery::new(
        Term::from_field_text(id_field_handle, &m.source_id),
        IndexRecordOption::Basic,
    );
    Ok(Box::new(BooleanQuery::new(vec![
        (Occur::Must, mlt),
        (Occur::MustNot, Box::new(exclude_term)),
    ])))
}
