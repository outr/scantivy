# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — 2026-04-26

Initial public release.

### Search

- Full query AST: `All`, `None`, `Term`, `Range`, `In`, `StartsWith`, `EndsWith`, `Contains`,
  `Regex`, `Exact`, `FullText`, `Phrase`, `DrillDown`, `Bool`, `Exists`, `Fuzzy`, `PhrasePrefix`,
  `Boost`, `DisjunctionMax`, `ConstScore`, `MoreLikeThis` (with `exclude_source` defaulting to
  true).
- Sort: relevance, by-fast-field, index-order, plus a multi-key sort collector that
  lexicographically combines arbitrary keys with mixed asc/desc directions.
- Snippet generation per field with custom pre/post tags and char limits.
- `explain` op returning Tantivy's pretty-JSON `Explanation` for a single doc/query pair.
- Conversion modes: full doc, single value, JSON, materialized subset.
- Distinct streaming with cursor paging.

### Aggregations

- Sum, Avg, Min, Max, Count, Stats, Cardinality, Histogram, Terms, Concat (custom path),
  DateHistogram, Range, Percentiles.
- Sub-aggregations on every bucket type (Terms / Histogram / DateHistogram / Range).

### Schema

- Field types: Text, String, Bool, I64, U64, F64, Date, Bytes, Facet, Json, IP.
- Tokenizers: `default`, `raw`, `en_stem`, `simple`, `whitespace`, `whitespace_raw`,
  `lowercase`, `ngram_3_10`, `english_stem`.

### Runtime

- Multi-arch native packaging: `linux-x86_64`, `linux-aarch64`, `macos-aarch64` (Apple
  Silicon), `windows-x86_64`. `NativeLibLoader` extracts the right binary on first use to a
  sha-keyed tmpdir cache. Intel-Mac (macOS 13 or older) is intentionally not supported.
- FFI uses the JEP 442 Foreign Function & Memory API (Java 22+).
- Every FFI entry point runs inside `std::panic::catch_unwind`; Rust panics surface as
  `error: Some("internal panic: ...")` instead of aborting the JVM.
- Tantivy diagnostics surface via `env_logger` — set `RUST_LOG=tantivy=info` (or `debug`) to
  see merge / segment / store messages on stderr.

### Known limitations

- No geo / spatial primitives (Tantivy has no native support; bbox-on-points is expressible
  via `QueryBool` of two range queries).
- No block-join (parent-child / nested) — Tantivy 0.26 has no block-join collector.
- No custom tokenizer pipelines (the named set above is what's available).
- No custom score-expression DSL (`tweak_score` / `custom_score` not exposed).

[1.0.0]: https://github.com/outr/scantivy/releases/tag/v1.0.0
