# Scantivy 1.0 — Status

## Done (1.0)
- [x] Tantivy 0.26.1, Rust 1.95+, prost 0.14, parking_lot, sha2/uuid for stable index ids
- [x] Scala 3.8.3 only (Scala 2.13 dropped); sbt 1.12.9
- [x] Legacy JSON FFI deleted (lib.rs, TantivyIndex.scala) — proto-only API
- [x] Proto schema: typed Value oneof, full filter/sort/conversion/aggregation/distinct coverage
- [x] Rust impl split into modules: error/schema/convert/query/search/aggregate/distinct/index
- [x] Per-index handle with long-lived IndexWriter + auto-refreshing IndexReader (no per-add commit)
- [x] FFI ops: create/open/drop, index/index_batch/upsert/delete/delete_by_query/truncate/optimize, commit/rollback, count/search/aggregate/distinct
- [x] Search collectors: TopDocs (relevance, by-fast-field, index-order), Count, FacetCollector, plus a multi-key sort collector that lexicographically combines score / index-order / fast-field keys
- [x] Query AST: All/None/Term/Range/InSet/StartsWith/EndsWith/Contains/Regex/Exact/FullText/Phrase/DrillDown/Bool/Exists/Fuzzy/PhrasePrefix/Boost/DisjunctionMax/ConstScore/MoreLikeThis
- [x] Snippet generation / highlighting via `tantivy::snippet::SnippetGenerator` (per-field pre/post tags + max char count)
- [x] `explain` FFI op returning Tantivy's pretty-JSON Explanation tree, looked up via the schema's `id_field`
- [x] Tokenizer registry: `default`/`raw`/`en_stem` plus `simple`/`whitespace`/`whitespace_raw`/`lowercase`/`ngram_3_10`/`english_stem` registered on every `IndexHandle`
- [x] Aggregations: Sum/Avg/Min/Max/Count/Stats/Cardinality/Histogram/Terms/Concat/DateHistogram/Range/Percentiles, with sub-aggregations attachable to any bucket type
- [x] Multi-arch native packaging via GitHub Actions matrix (linux-x86_64, linux-aarch64, macos-aarch64, windows-x86_64)
- [x] NativeLibLoader: extracts from `/native/<os>-<arch>/` jar resources to tmpdir cache (sha-keyed)
- [x] FFM (JEP 442) replaces JNR-FFI; Java 22+ floor
- [x] Sonatype publish wiring (sbt-sonatype + sbt-pgp), `Tantivy.create` / `Tantivy.open` / `TantivyIndex` Scala 3 facade
- [x] Smoke tests: 32/32 passing (Scala) + 3/3 (Rust unit tests for `run_ffi`)
- [x] `Tantivy.open` accepts `id_field` so reopened indexes can use upsert/delete-by-id/MoreLikeThis/explain
- [x] LICENSE (MIT), README.md, CHANGELOG.md, RELEASING.md added; versions at 1.0.0
- [x] Every FFI entry point wraps in `std::panic::catch_unwind` — Rust panics surface as `error: Some("internal panic: …")` instead of aborting the JVM
- [x] `env_logger` initialized lazily so `RUST_LOG=tantivy=info` surfaces Tantivy diagnostics
- [x] Per-method Scaladoc on `Tantivy` / `TantivyIndex` for the published artifact docs
- [x] `release.yml` matrix now covers `macos-x86_64` (was only `macos-aarch64`)
- [x] Concurrency model + Tantivy-version coupling documented in README.md and CLAUDE.md

## Known limitations (intentional)
- Geo / spatial — Tantivy has no native support. Bounding-box-on-points can be expressed today via a `QueryBool` of two range queries on lat/lon fast fields; distance / contains / intersects are not implemented.
- Block-join (parent-child / nested) — Tantivy 0.26 has no block-join collector; not implemented.
- `MoreLikeThis` excludes the source doc from results by default (Scantivy wraps the inner Tantivy query in a `BooleanQuery must_not` on the source id). Set `exclude_source = false` to opt back into Tantivy's raw "source ranks first" behavior.
- `DocSetCollector` (raw `HashSet<DocAddress>` enumeration) intentionally not surfaced — `search` with a high `limit` and unsorted relevance covers the same ground without adding API surface.
- Custom tokenizer registration (user-supplied filters/pipelines) is not exposed; the named set above is what's available. Adding more would require a richer schema-level analyzer DSL.
