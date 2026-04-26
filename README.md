# Scantivy

A Scala 3 wrapper around [Tantivy](https://github.com/quickwit-oss/tantivy), Rust's full-text search engine. The Rust side is compiled as a `cdylib` and the Scala side talks to it over a protobuf-encoded FFI using the JEP 442 Foreign Function & Memory API.

## Requirements

- **Java 22+** (the FFM API was finalized in JDK 22)
- **Scala 3.8+**
- A pre-built native lib for your platform — Scantivy publishes multi-arch jars containing `linux-x86_64`, `linux-aarch64`, `macos-aarch64` (Apple Silicon), and `windows-x86_64` binaries; the loader picks the right one at runtime. Intel-Mac users on macOS 13 or older are not supported.

## Adding to a project

```scala
libraryDependencies += "com.outr" %% "scantivy" % "1.0.0"

// FFM marks `SymbolLookup.libraryLookup` as restricted; opt the unnamed module in once.
javaOptions += "--enable-native-access=ALL-UNNAMED"
```

If your app runs from a named JPMS module, replace `ALL-UNNAMED` with that module's name.

## Quick example

```scala
import scantivy.*
import scantivy.proto as pb

val schema = pb.SchemaDef(fields = Seq(
  pb.FieldDef(name = "id",   kind = pb.FieldKind.STRING, stored = true, indexed = true, fast = true),
  pb.FieldDef(name = "name", kind = pb.FieldKind.TEXT,   stored = true, indexed = true, tokenized = true),
  pb.FieldDef(name = "age",  kind = pb.FieldKind.I64,    stored = true, indexed = true, fast = true)
))

val index = Tantivy.create(pb.CreateIndexRequest(
  schema  = Some(schema),
  idField = Some("id")          // enables upsert / delete-by-id / explain / MoreLikeThis
)).fold(e => sys.error(e), identity)

try {
  index.indexBatch(Seq(
    pb.Document(fields = Seq(
      pb.FieldValue(name = "id",   values = Seq(pb.Value(pb.Value.Kind.StringValue("a")))),
      pb.FieldValue(name = "name", values = Seq(pb.Value(pb.Value.Kind.StringValue("Alice In Wonderland")))),
      pb.FieldValue(name = "age",  values = Seq(pb.Value(pb.Value.Kind.LongValue(30))))
    ))
  )).fold(e => sys.error(e), identity)
  index.commit().fold(e => sys.error(e), identity)

  val q = pb.Query(pb.Query.Node.FullText(pb.QueryFullText(
    query  = "wonderland",
    fields = Seq("name")
  )))
  val resp = index.search(pb.SearchRequest(
    query    = Some(q),
    limit    = 10,
    snippets = Seq(pb.SnippetSpec(field = "name"))
  )).fold(e => sys.error(e), identity)

  resp.hits.foreach(h => println(s"${h.id.get}: ${h.snippets.head.fragment}"))
} finally index.close()
```

For an on-disk index, set `path` on `CreateIndexRequest`. Reopen later with:

```scala
val index = Tantivy.open(path = "/var/data/my-index", idField = Some("id")).fold(...)
```

The `idField` argument is required at open time if you need any id-based ops — Tantivy's on-disk format doesn't persist it.

## What's in the box

- **Query AST**: `All`, `None`, `Term`, `Range`, `In`, `StartsWith`, `EndsWith`, `Contains`, `Regex`, `Exact`, `FullText`, `Phrase`, `DrillDown`, `Bool`, `Exists`, `Fuzzy`, `PhrasePrefix`, `Boost`, `DisjunctionMax`, `ConstScore`, `MoreLikeThis`.
- **Sort**: relevance, by-fast-field, index-order, plus multi-key sort that lexicographically combines arbitrary keys.
- **Aggregations**: Sum / Avg / Min / Max / Count / Stats / Cardinality / Histogram / Terms / Concat / DateHistogram / Range / Percentiles, with sub-aggregations on every bucket type.
- **Snippets / highlighting** with per-field pre/post tags and char limits.
- **Explain** returning Tantivy's pretty-JSON score breakdown for any (query, doc) pair.
- **Tokenizers**: `default`, `raw`, `en_stem`, `simple`, `whitespace`, `whitespace_raw`, `lowercase`, `ngram_3_10`, `english_stem`.
- **Conversion modes**: full doc / single value / JSON / materialized subset.
- **Distinct** streaming with cursor paging.

## Concurrency

`TantivyIndex` is thread-safe and shareable. Internally:

- **Writer ops** (`index`, `indexBatch`, `upsert`, `delete`, `truncate`, `commit`, `rollback`)
  serialize on a per-index `Mutex` over Tantivy's `IndexWriter`. Multiple threads can call them
  concurrently; they will queue.
- **Reader ops** (`count`, `search`, `aggregate`, `distinct`, `explain`) run concurrently against
  Tantivy's auto-refreshing `IndexReader`. The reader snapshot is updated on commit (with a small
  delay) — readers see writes only after the writer thread calls [[TantivyIndex.commit]].
- The handle registry that maps `index.id` → `IndexHandle` is `RwLock`-guarded, so multiple
  indexes running ops at the same time don't contend.

Uncommitted writes are lost if the JVM exits without [[TantivyIndex.close]] (or `commit`)
running first. `close` does a best-effort commit before dropping the registry entry.

## Diagnostics

Tantivy emits internal warnings (segment merges, store decode errors, etc.) via the `log` crate.
Scantivy initializes `env_logger` lazily on first index creation, so set:

```bash
RUST_LOG=tantivy=info   # or =debug for more
```

before launching the JVM and Tantivy's diagnostics will land on stderr. If your application has
already installed a `log` adapter on the Rust side (rare from a JVM consumer), our `try_init` is
a no-op.

## Index format & version compatibility

Scantivy 1.x pins **Tantivy 0.26**. The on-disk index format is whatever Tantivy 0.26 writes —
Tantivy doesn't promise format stability across its own minor versions, so a future Scantivy
release that bumps the Tantivy dependency may require reindexing. We'll call this out explicitly
in the CHANGELOG when it happens. Stay on Scantivy 1.x for the lifetime of an existing index.

## Known limitations

- **Geo / spatial** — Tantivy has no native support. Bounding-box-on-points works today via two range queries on lat/lon fast fields; distance / contains / intersects are not implemented.
- **Block-join (parent-child / nested)** — Tantivy 0.26 has no block-join collector; not implemented.
- **Custom tokenizer pipelines** — the named set above is what's available; user-supplied filter chains are not exposed.
- **Custom score expressions** (`tweak_score`/`custom_score`) — would need an expression DSL; not in scope for 1.0.

## License

MIT — see [LICENSE](./LICENSE).
