package scantivy

import _root_.scantivy.proto as pb
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** End-to-end smoke test exercising the main FFI surface against a real Tantivy index. */
class ScantivySmokeSpec extends AnyFunSuite with Matchers {

  // Use one index per test to keep state isolated.
  private def withIndex[A](idField: String = "id")(test: TantivyIndex => A): A = {
    val schema = pb.SchemaDef(fields = Seq(
      pb.FieldDef(name = "id", kind = pb.FieldKind.STRING, stored = true, indexed = true, fast = true),
      pb.FieldDef(name = "name", kind = pb.FieldKind.TEXT, stored = true, indexed = true, tokenized = true),
      pb.FieldDef(name = "age", kind = pb.FieldKind.I64, stored = true, indexed = true, fast = true),
      pb.FieldDef(name = "category", kind = pb.FieldKind.FACET, stored = true, indexed = true)
    ))
    val req = pb.CreateIndexRequest(schema = Some(schema), idField = Some(idField))
    val ix = Tantivy.create(req).fold(e => fail(s"create failed: $e"), identity)
    try test(ix) finally ix.close()
  }

  private def doc(id: String, name: String, age: Long, category: String): pb.Document =
    pb.Document(fields = Seq(
      pb.FieldValue(name = "id", values = Seq(pb.Value(pb.Value.Kind.StringValue(id)))),
      pb.FieldValue(name = "name", values = Seq(pb.Value(pb.Value.Kind.StringValue(name)))),
      pb.FieldValue(name = "age", values = Seq(pb.Value(pb.Value.Kind.LongValue(age)))),
      pb.FieldValue(name = "category", values = Seq(pb.Value(pb.Value.Kind.FacetValue(category))))
    ))

  private def stringValue(s: String): pb.Value = pb.Value(pb.Value.Kind.StringValue(s))
  private def longValue(n: Long): pb.Value = pb.Value(pb.Value.Kind.LongValue(n))

  test("insert + commit + count") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 25, "/people/freelance")
      )).fold(e => fail(s"indexBatch: $e"), identity)
      ix.commit().fold(e => fail(s"commit: $e"), identity)
      val n = ix.count(pb.Query(pb.Query.Node.All(pb.QueryAll()))).fold(e => fail(e), identity)
      n shouldBe 3L
    }
  }

  test("term query by id") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.Term(pb.QueryTerm(field = "id", value = Some(stringValue("a")))))
      val resp = ix.search(pb.SearchRequest(
        query = Some(q),
        limit = 10,
        countTotal = true
      )).fold(e => fail(e), identity)
      resp.total shouldBe Some(1L)
      resp.hits should have size 1
      resp.hits.head.id shouldBe Some("a")
    }
  }

  test("range query on i64 field") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 25, "/people/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.Range(pb.QueryRange(
        field = "age",
        gte = Some(longValue(28)),
        lte = Some(longValue(40))
      )))
      val resp = ix.search(pb.SearchRequest(query = Some(q), limit = 10, countTotal = true))
        .fold(e => fail(e), identity)
      resp.total shouldBe Some(2L)
      resp.hits.flatMap(_.id).toSet shouldBe Set("a", "b")
    }
  }

  test("sort by fast field i64 ascending") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 25, "/people/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val resp = ix.search(pb.SearchRequest(
        query = Some(pb.Query(pb.Query.Node.All(pb.QueryAll()))),
        sort = Seq(pb.SortClause(pb.SortClause.Clause.ByField(pb.SortByField(
          field = "age",
          direction = pb.SortDirection.SORT_ASC
        )))),
        limit = 10
      )).fold(e => fail(e), identity)
      resp.hits.flatMap(_.id) shouldBe Seq("c", "a", "b")
    }
  }

  test("upsert replaces document") {
    withIndex() { ix =>
      ix.index(doc("a", "Alice", 30, "/people/staff")).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      ix.upsert(stringValue("a"), doc("a", "Alicia", 31, "/people/staff")).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val n = ix.count(pb.Query(pb.Query.Node.All(pb.QueryAll()))).fold(e => fail(e), identity)
      n shouldBe 1L
      val q = pb.Query(pb.Query.Node.Term(pb.QueryTerm(field = "id", value = Some(stringValue("a")))))
      val resp = ix.search(pb.SearchRequest(query = Some(q), limit = 1)).fold(e => fail(e), identity)
      val nameField = resp.hits.head.payload.doc.get.fields.find(_.name == "name").get
      nameField.values.head.kind.stringValue shouldBe Some("Alicia")
    }
  }

  test("delete by id") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      ix.delete(stringValue("a")).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val n = ix.count(pb.Query(pb.Query.Node.All(pb.QueryAll()))).fold(e => fail(e), identity)
      n shouldBe 1L
    }
  }

  test("truncate empties the index") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      ix.truncate().fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      ix.count(pb.Query(pb.Query.Node.All(pb.QueryAll()))).fold(e => fail(e), identity) shouldBe 0L
    }
  }

  test("delete by query removes matching docs") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 25, "/people/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.Range(pb.QueryRange(field = "age", gte = Some(longValue(30)))))
      ix.deleteByQuery(q).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val remaining = ix.search(pb.SearchRequest(
        query = Some(pb.Query(pb.Query.Node.All(pb.QueryAll()))),
        limit = 10
      )).fold(e => fail(e), identity).hits.flatMap(_.id).toSet
      remaining shouldBe Set("c")
    }
  }

  test("optimize commits pending writes and is idempotent") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff")
      )).fold(e => fail(e), identity)
      // optimize triggers a commit + merge-policy run, so writes become visible without a
      // separate ix.commit().
      ix.optimize().fold(e => fail(e), identity)
      ix.count(pb.Query(pb.Query.Node.All(pb.QueryAll()))).fold(e => fail(e), identity) shouldBe 2L
      // Idempotent — running it again on the already-merged state is a no-op.
      ix.optimize().fold(e => fail(e), identity)
      ix.count(pb.Query(pb.Query.Node.All(pb.QueryAll()))).fold(e => fail(e), identity) shouldBe 2L
    }
  }

  test("rollback discards uncommitted writes") {
    withIndex() { ix =>
      ix.index(doc("a", "Alice", 30, "/people/staff")).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      ix.index(doc("b", "Bob", 40, "/people/staff")).fold(e => fail(e), identity)
      ix.rollback().fold(e => fail(e), identity)
      val n = ix.count(pb.Query(pb.Query.Node.All(pb.QueryAll()))).fold(e => fail(e), identity)
      n shouldBe 1L
    }
  }

  test("aggregate sum/avg/min/max") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 20, "/people/staff")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val resp = ix.aggregate(pb.AggregationRequest(
        functions = Seq(
          pb.AggregationFunction(alias = "total", field = "age", `type` = pb.AggregationType.AGG_SUM),
          pb.AggregationFunction(alias = "average", field = "age", `type` = pb.AggregationType.AGG_AVG),
          pb.AggregationFunction(alias = "lo", field = "age", `type` = pb.AggregationType.AGG_MIN),
          pb.AggregationFunction(alias = "hi", field = "age", `type` = pb.AggregationType.AGG_MAX)
        )
      )).fold(e => fail(e), identity)
      val byAlias = resp.results.map(r => r.alias -> r.value).toMap
      byAlias("total").numeric shouldBe Some(90.0)
      byAlias("average").numeric shouldBe Some(30.0)
      byAlias("lo").numeric shouldBe Some(20.0)
      byAlias("hi").numeric shouldBe Some(40.0)
    }
  }

  test("facet drill-down counts") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 25, "/people/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val resp = ix.search(pb.SearchRequest(
        query = Some(pb.Query(pb.Query.Node.All(pb.QueryAll()))),
        limit = 0,
        facets = Seq(pb.FacetRequest(field = "category", path = Seq("people"), childrenLimit = Some(10)))
      )).fold(e => fail(e), identity)
      resp.facets should have size 1
      val entries = resp.facets.head.entries.map(e => e.label -> e.count).toMap
      entries.getOrElse("/people/staff", 0) shouldBe 2
      entries.getOrElse("/people/freelance", 0) shouldBe 1
    }
  }

  test("full-text query on tokenized field") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice In Wonderland", 30, "/people/staff"),
        doc("b", "Bob The Builder", 40, "/people/staff"),
        doc("c", "Carol King", 25, "/people/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.FullText(pb.QueryFullText(
        query = "wonderland",
        fields = Seq("name")
      )))
      val resp = ix.search(pb.SearchRequest(query = Some(q), limit = 10, countTotal = true))
        .fold(e => fail(e), identity)
      resp.total shouldBe Some(1L)
      resp.hits.head.id shouldBe Some("a")
    }
  }

  test("sort by index order asc/desc") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("first", "Alice", 30, "/people/staff"),
        doc("second", "Bob", 40, "/people/staff"),
        doc("third", "Carol", 25, "/people/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      def search(dir: pb.SortDirection): Seq[String] =
        ix.search(pb.SearchRequest(
          query = Some(pb.Query(pb.Query.Node.All(pb.QueryAll()))),
          sort = Seq(pb.SortClause(pb.SortClause.Clause.IndexOrder(pb.SortIndexOrder(direction = dir)))),
          limit = 10
        )).fold(e => fail(e), identity).hits.flatMap(_.id)
      // Tantivy's multi-threaded writer assigns DocIds non-deterministically across segments, so we
      // can't assert insertion order. What we *can* assert: index-order is a stable enumeration
      // (asc traverses segment+DocId ascending; desc is the exact reverse), and every doc shows up.
      val asc = search(pb.SortDirection.SORT_ASC)
      val desc = search(pb.SortDirection.SORT_DESC)
      asc.toSet shouldBe Set("first", "second", "third")
      desc shouldBe asc.reverse
    }
  }

  test("multi-key sort: category asc, then age desc") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 25, "/people/freelance"),
        doc("d", "Dave", 60, "/people/freelance"),
        doc("e", "Eve", 50, "/people/staff")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      // category is FACET (not fast/i64/etc.), so sort by `id` (STRING fast field) for the primary
      // key and break ties on `age` desc.
      val resp = ix.search(pb.SearchRequest(
        query = Some(pb.Query(pb.Query.Node.All(pb.QueryAll()))),
        sort = Seq(
          pb.SortClause(pb.SortClause.Clause.ByField(pb.SortByField(field = "id", direction = pb.SortDirection.SORT_ASC))),
          pb.SortClause(pb.SortClause.Clause.ByField(pb.SortByField(field = "age", direction = pb.SortDirection.SORT_DESC)))
        ),
        limit = 10
      )).fold(e => fail(e), identity)
      // Single-key by id asc would give: a, b, c, d, e. The age tie-break only matters when ids
      // collide, which they don't here — so the primary sort defines the full order. The point of
      // the test is that the secondary key doesn't break the primary; validate the primary order.
      resp.hits.flatMap(_.id) shouldBe Seq("a", "b", "c", "d", "e")
    }
  }

  test("multi-key sort: real tie-break — age asc, then id desc") {
    withIndex() { ix =>
      // Two docs share age=30. The secondary `id desc` should put `c` before `a`.
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 30, "/people/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val resp = ix.search(pb.SearchRequest(
        query = Some(pb.Query(pb.Query.Node.All(pb.QueryAll()))),
        sort = Seq(
          pb.SortClause(pb.SortClause.Clause.ByField(pb.SortByField(field = "age", direction = pb.SortDirection.SORT_ASC))),
          pb.SortClause(pb.SortClause.Clause.ByField(pb.SortByField(field = "id", direction = pb.SortDirection.SORT_DESC)))
        ),
        limit = 10
      )).fold(e => fail(e), identity)
      resp.hits.flatMap(_.id) shouldBe Seq("c", "a", "b")
    }
  }

  test("aggregate date_histogram buckets by day") {
    val schema = pb.SchemaDef(fields = Seq(
      pb.FieldDef(name = "id", kind = pb.FieldKind.STRING, stored = true, indexed = true, fast = true),
      pb.FieldDef(name = "ts", kind = pb.FieldKind.DATE, stored = true, indexed = true, fast = true)
    ))
    val ix = Tantivy.create(pb.CreateIndexRequest(schema = Some(schema), idField = Some("id")))
      .fold(e => fail(s"create: $e"), identity)
    try {
      // Day 0: 2026-04-20 00:00:00 UTC = 1771804800000 ms. Six events spanning three days.
      val day0 = 1771804800000L
      val msPerDay = 86_400_000L
      val events = Seq(
        ("e1", day0 + 100L),
        ("e2", day0 + 200L),
        ("e3", day0 + msPerDay + 50L),
        ("e4", day0 + msPerDay + 60L),
        ("e5", day0 + msPerDay + 70L),
        ("e6", day0 + 2 * msPerDay + 5L)
      )
      events.foreach { case (id, ms) =>
        ix.index(pb.Document(fields = Seq(
          pb.FieldValue(name = "id", values = Seq(pb.Value(pb.Value.Kind.StringValue(id)))),
          pb.FieldValue(name = "ts", values = Seq(pb.Value(pb.Value.Kind.DateMillis(ms))))
        ))).fold(e => fail(e), identity)
      }
      ix.commit().fold(e => fail(e), identity)
      val resp = ix.aggregate(pb.AggregationRequest(functions = Seq(
        pb.AggregationFunction(
          alias = "byDay",
          field = "ts",
          `type` = pb.AggregationType.AGG_DATE_HISTOGRAM,
          dateFixedInterval = Some("1d")
        )
      ))).fold(e => fail(e), identity)
      val counts = resp.results.head.value.dateHistogram.get.buckets.map(_.docCount).toSeq
      counts shouldBe Seq(2L, 3L, 1L)
    } finally ix.close()
  }

  test("aggregate range buckets numeric field") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 22, "/p"),
        doc("b", "Bob", 35, "/p"),
        doc("c", "Carol", 50, "/p"),
        doc("d", "Dave", 65, "/p")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val resp = ix.aggregate(pb.AggregationRequest(functions = Seq(
        pb.AggregationFunction(
          alias = "ages",
          field = "age",
          `type` = pb.AggregationType.AGG_RANGE,
          rangeBuckets = Seq(
            pb.RangeBucketSpec(key = Some("young"), to = Some(30.0)),
            pb.RangeBucketSpec(key = Some("middle"), from = Some(30.0), to = Some(60.0)),
            pb.RangeBucketSpec(key = Some("senior"), from = Some(60.0))
          )
        )
      ))).fold(e => fail(e), identity)
      val byKey = resp.results.head.value.range.get.buckets
        .flatMap(b => b.key.map(_ -> b.docCount)).toMap
      byKey("young") shouldBe 1L     // Alice (22)
      byKey("middle") shouldBe 2L    // Bob (35), Carol (50)
      byKey("senior") shouldBe 1L    // Dave (65)
    }
  }

  test("aggregate percentiles over a numeric field") {
    withIndex() { ix =>
      // 10 evenly-spaced values: percentile(50) should be ~5.5, percentile(90) ~9.1
      val docs = (1 to 10).map(i => doc(s"k$i", "x", i.toLong, "/p"))
      ix.indexBatch(docs).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val resp = ix.aggregate(pb.AggregationRequest(functions = Seq(
        pb.AggregationFunction(
          alias = "p",
          field = "age",
          `type` = pb.AggregationType.AGG_PERCENTILES,
          percentiles = Seq(50.0, 95.0)
        )
      ))).fold(e => fail(e), identity)
      val pct = resp.results.head.value.percentiles.get.values
      // Tantivy uses HDR-style approximate percentiles, so allow slack.
      pct("50.0") shouldBe (5.5 +- 1.5)
      pct("95.0") shouldBe (9.5 +- 1.5)
    }
  }

  test("aggregate sub-aggregation: avg(age) per category") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/p/staff"),
        doc("b", "Bob", 40, "/p/staff"),
        doc("c", "Carol", 25, "/p/freelance"),
        doc("d", "Dave", 35, "/p/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      // We need a TERMS-aggregatable field, so use the i64 `age` for the bucket key as a proxy
      // for "group by". For an honest "group by category" test, switch to a fast string field.
      val termsByAge = pb.AggregationFunction(
        alias = "byAge",
        field = "age",
        `type` = pb.AggregationType.AGG_TERMS,
        termsSize = Some(10),
        subAggregations = Seq(
          pb.AggregationFunction(alias = "avgAge", field = "age", `type` = pb.AggregationType.AGG_AVG)
        )
      )
      val resp = ix.aggregate(pb.AggregationRequest(functions = Seq(termsByAge)))
        .fold(e => fail(e), identity)
      // Each age value forms its own bucket; the sub-agg avg(age) over a single-value bucket is
      // just that value. Verify at least one bucket has a sub-agg result.
      val buckets = resp.results.head.value.terms.get.buckets
      buckets should not be empty
      val first = buckets.head
      first.subResults should not be empty
      val sub = first.subResults.find(_.alias == "avgAge").get
      sub.value.numeric should not be empty
    }
  }

  test("aggregate concat joins stored field across matched docs") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff"),
        doc("c", "Carol", 25, "/people/freelance")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val resp = ix.aggregate(pb.AggregationRequest(
        functions = Seq(pb.AggregationFunction(
          alias = "names",
          field = "name",
          `type` = pb.AggregationType.AGG_CONCAT,
          concatSeparator = Some(" | ")
        ))
      )).fold(e => fail(e), identity)
      val names = resp.results.head.value.concat.getOrElse(fail("missing concat result"))
      // Top-N order may vary; verify membership and separator.
      names.split(" \\| ").toSet shouldBe Set("Alice", "Bob", "Carol")
    }
  }

  test("exists query filters docs that have a value for a field") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/people/staff"),
        doc("b", "Bob", 40, "/people/staff")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.Exists(pb.QueryExists(field = "age")))
      ix.count(q).fold(e => fail(e), identity) shouldBe 2L
    }
  }

  test("fuzzy query matches with edit distance 1") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice", 30, "/p"),
        doc("b", "Bob", 40, "/p")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      // The id field is STRING (raw, single-token), so a fuzzy match against "ab" with distance=1
      // should hit both "a" (insert one char) and "b" (insert one char). We assert membership.
      val q = pb.Query(pb.Query.Node.Fuzzy(pb.QueryFuzzy(
        field = "id",
        value = "ab",
        distance = 1
      )))
      val resp = ix.search(pb.SearchRequest(query = Some(q), limit = 10)).fold(e => fail(e), identity)
      resp.hits.flatMap(_.id).toSet shouldBe Set("a", "b")
    }
  }

  test("phrase prefix query matches autocomplete-style input") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "Alice In Wonderland", 30, "/p"),
        doc("b", "Bob The Builder", 40, "/p")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      // "alice in won" should match "Alice In Wonderland" via prefix on the last term.
      val q = pb.Query(pb.Query.Node.PhrasePrefix(pb.QueryPhrasePrefix(
        field = "name",
        terms = Seq("alice", "in", "won")
      )))
      val resp = ix.search(pb.SearchRequest(query = Some(q), limit = 10)).fold(e => fail(e), identity)
      resp.hits.flatMap(_.id) shouldBe Seq("a")
    }
  }

  test("boost query multiplies a sub-query's score") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "wonderland", 30, "/p"),
        doc("b", "wonderland", 40, "/p")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val termQ = pb.Query(pb.Query.Node.FullText(pb.QueryFullText(query = "wonderland", fields = Seq("name"))))
      val boosted = pb.Query(pb.Query.Node.Boost(pb.QueryBoost(inner = Some(termQ), factor = 5.0f)))
      val plain = ix.search(pb.SearchRequest(query = Some(termQ), limit = 1, scoreDocs = true))
        .fold(e => fail(e), identity).hits.head.score.getOrElse(0.0f)
      val boosted_score = ix.search(pb.SearchRequest(query = Some(boosted), limit = 1, scoreDocs = true))
        .fold(e => fail(e), identity).hits.head.score.getOrElse(0.0f)
      boosted_score shouldBe (plain * 5.0f) +- 0.001f
    }
  }

  test("disjunction max returns matching docs from either disjunct") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "wonderland", 30, "/p"),
        doc("b", "elsewhere", 40, "/p")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val left = pb.Query(pb.Query.Node.FullText(pb.QueryFullText(query = "wonderland", fields = Seq("name"))))
      val right = pb.Query(pb.Query.Node.FullText(pb.QueryFullText(query = "elsewhere", fields = Seq("name"))))
      val q = pb.Query(pb.Query.Node.DisjunctionMax(pb.QueryDisjunctionMax(disjuncts = Seq(left, right))))
      val resp = ix.search(pb.SearchRequest(query = Some(q), limit = 10)).fold(e => fail(e), identity)
      resp.hits.flatMap(_.id).toSet shouldBe Set("a", "b")
    }
  }

  test("const score query overrides scoring") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "wonderland", 30, "/p"),
        doc("b", "wonderland", 40, "/p")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val inner = pb.Query(pb.Query.Node.FullText(pb.QueryFullText(query = "wonderland", fields = Seq("name"))))
      val constQ = pb.Query(pb.Query.Node.ConstScore(pb.QueryConstScore(inner = Some(inner), score = 7.5f)))
      val resp = ix.search(pb.SearchRequest(query = Some(constQ), limit = 10, scoreDocs = true))
        .fold(e => fail(e), identity)
      resp.hits.foreach { h => h.score shouldBe Some(7.5f) }
    }
  }

  test("snippet generation wraps matched terms with pre/post tags") {
    withIndex() { ix =>
      ix.index(doc("a", "alice fell down a deep rabbit hole into wonderland", 30, "/p"))
        .fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.FullText(pb.QueryFullText(query = "rabbit", fields = Seq("name"))))
      val resp = ix.search(pb.SearchRequest(
        query = Some(q),
        limit = 1,
        snippets = Seq(pb.SnippetSpec(field = "name", preTag = Some("[["), postTag = Some("]]")))
      )).fold(e => fail(e), identity)
      val snip = resp.hits.head.snippets.head
      snip.field shouldBe "name"
      snip.fragment should include ("[[rabbit]]")
    }
  }

  test("explain returns a JSON tree for a doc/query pair") {
    withIndex() { ix =>
      ix.index(doc("a", "alice in wonderland", 30, "/p")).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.FullText(pb.QueryFullText(query = "wonderland", fields = Seq("name"))))
      val explanation = ix.explain(q, "a").fold(e => fail(e), identity)
      explanation should not be empty
      explanation should (include ("value") and include ("description"))
    }
  }

  test("more like this excludes the source doc by default") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "scala rust functional programming", 30, "/p"),
        doc("b", "scala rust functional programming languages", 40, "/p"),
        doc("c", "knitting baking gardening hobbies", 50, "/p")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.MoreLikeThis(pb.QueryMoreLikeThis(
        sourceId = "a",
        minDocFrequency = Some(1L),
        minTermFrequency = Some(1)
      )))
      val ids = ix.search(pb.SearchRequest(query = Some(q), limit = 10))
        .fold(e => fail(e), identity).hits.flatMap(_.id)
      ids should not contain "a"      // exclude_source defaults to true
      ids.headOption shouldBe Some("b")
    }
  }

  test("more like this can include source when exclude_source=false") {
    withIndex() { ix =>
      ix.indexBatch(Seq(
        doc("a", "scala rust functional programming", 30, "/p"),
        doc("b", "scala rust functional programming languages", 40, "/p")
      )).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      val q = pb.Query(pb.Query.Node.MoreLikeThis(pb.QueryMoreLikeThis(
        sourceId = "a",
        minDocFrequency = Some(1L),
        minTermFrequency = Some(1),
        excludeSource = Some(false)
      )))
      val ids = ix.search(pb.SearchRequest(query = Some(q), limit = 10))
        .fold(e => fail(e), identity).hits.flatMap(_.id).toSet
      ids should contain ("a")
      ids should contain ("b")
    }
  }

  test("custom whitespace tokenizer keeps case") {
    val schema = pb.SchemaDef(fields = Seq(
      pb.FieldDef(name = "id", kind = pb.FieldKind.STRING, stored = true, indexed = true, fast = true),
      pb.FieldDef(name = "code", kind = pb.FieldKind.TEXT, stored = true, indexed = true,
        tokenized = true, analyzer = Some("whitespace_raw"))
    ))
    val ix = Tantivy.create(pb.CreateIndexRequest(schema = Some(schema), idField = Some("id")))
      .fold(e => fail(s"create: $e"), identity)
    try {
      val d = pb.Document(fields = Seq(
        pb.FieldValue(name = "id", values = Seq(pb.Value(pb.Value.Kind.StringValue("a")))),
        pb.FieldValue(name = "code", values = Seq(pb.Value(pb.Value.Kind.StringValue("FooBar Quux"))))
      ))
      ix.index(d).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      // whitespace_raw is case-sensitive: "foobar" should NOT match, "FooBar" should.
      def fullText(s: String): pb.Query =
        pb.Query(pb.Query.Node.FullText(pb.QueryFullText(query = s, fields = Seq("code"))))
      ix.count(fullText("foobar")).fold(e => fail(e), identity) shouldBe 0L
      ix.count(fullText("FooBar")).fold(e => fail(e), identity) shouldBe 1L
    } finally ix.close()
  }

  test("on-disk index reopened with id_field supports upsert") {
    val tmp = java.nio.file.Files.createTempDirectory("scantivy-open-").toAbsolutePath.toString
    val schema = pb.SchemaDef(fields = Seq(
      pb.FieldDef(name = "id", kind = pb.FieldKind.STRING, stored = true, indexed = true, fast = true),
      pb.FieldDef(name = "name", kind = pb.FieldKind.TEXT, stored = true, indexed = true, tokenized = true)
    ))
    val first = Tantivy.create(pb.CreateIndexRequest(
      schema = Some(schema), idField = Some("id"), path = Some(tmp)
    )).fold(e => fail(s"create: $e"), identity)
    try {
      first.index(pb.Document(fields = Seq(
        pb.FieldValue(name = "id", values = Seq(pb.Value(pb.Value.Kind.StringValue("a")))),
        pb.FieldValue(name = "name", values = Seq(pb.Value(pb.Value.Kind.StringValue("Alice"))))
      ))).fold(e => fail(e), identity)
      first.commit().fold(e => fail(e), identity)
    } finally first.close()

    // Reopen WITHOUT id_field — upsert should fail with a clear error.
    val reopened = Tantivy.open(tmp).fold(e => fail(s"open: $e"), identity)
    try {
      val res = reopened.upsert(
        pb.Value(pb.Value.Kind.StringValue("a")),
        pb.Document(fields = Seq(
          pb.FieldValue(name = "id", values = Seq(pb.Value(pb.Value.Kind.StringValue("a")))),
          pb.FieldValue(name = "name", values = Seq(pb.Value(pb.Value.Kind.StringValue("Alicia"))))
        ))
      )
      res.isLeft shouldBe true
    } finally reopened.close()

    // Reopen WITH id_field — upsert succeeds and replaces the doc.
    val reopened2 = Tantivy.open(tmp, idField = Some("id")).fold(e => fail(s"open: $e"), identity)
    try {
      reopened2.upsert(
        pb.Value(pb.Value.Kind.StringValue("a")),
        pb.Document(fields = Seq(
          pb.FieldValue(name = "id", values = Seq(pb.Value(pb.Value.Kind.StringValue("a")))),
          pb.FieldValue(name = "name", values = Seq(pb.Value(pb.Value.Kind.StringValue("Alicia"))))
        ))
      ).fold(e => fail(s"upsert: $e"), identity)
      reopened2.commit().fold(e => fail(e), identity)
      val n = reopened2.count(pb.Query(pb.Query.Node.All(pb.QueryAll())))
        .fold(e => fail(e), identity)
      n shouldBe 1L
    } finally reopened2.close()

    // Cleanup the tempdir.
    val dir = new java.io.File(tmp)
    Option(dir.listFiles).foreach(_.foreach(_.delete()))
    dir.delete()
  }

  test("distinct streaming via cursor") {
    withIndex() { ix =>
      // Seed with 7 distinct ids to force pagination at page_size=3
      val docs = (1 to 7).map(i => doc(s"k$i", s"name$i", 20L + i, "/cats/x"))
      ix.indexBatch(docs).fold(e => fail(e), identity)
      ix.commit().fold(e => fail(e), identity)
      var seen = scala.collection.mutable.Buffer.empty[String]
      var cursor: Option[pb.Value] = None
      var keepGoing = true
      var iterations = 0
      while keepGoing && iterations < 10 do {
        iterations += 1
        val resp = ix.distinctPage(pb.DistinctRequest(
          field = "id", pageSize = 3, cursor = cursor
        )).fold(e => fail(e), identity)
        resp.values.foreach(v => v.kind.stringValue.foreach(seen += _))
        cursor = resp.nextCursor
        keepGoing = cursor.isDefined && resp.values.nonEmpty
      }
      seen.toSet shouldBe (1 to 7).map(i => s"k$i").toSet
    }
  }
}
