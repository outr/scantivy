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
