package scantivy

import _root_.scantivy.proto as pb

/** Handle to an open Tantivy index.
 *
 *  Cheap to clone (just wraps a string id into the global registry on the Rust side) and
 *  thread-safe — write ops are mutex-serialized per index, read ops run concurrently against an
 *  auto-refreshing reader snapshot.
 *
 *  Lifecycle:
 *  - Created by [[Tantivy.create]] / [[Tantivy.open]].
 *  - Closed via [[close]], which removes the entry from the Rust-side registry. Any pending
 *    uncommitted writes are best-effort committed on close.
 *
 *  Errors: every method returns `Either[String, T]` rather than throwing. The `Left` carries the
 *  error message produced by the Rust side — schema mismatches, missing fields, invalid query
 *  shapes, IO errors against on-disk indexes, and Tantivy-internal failures all flow through the
 *  same channel. A panic on the Rust side surfaces as `Left("internal panic: ...")` rather than
 *  killing the JVM.
 */
final class TantivyIndex private[scantivy] (val id: String) extends AutoCloseable {

  // ---- write ops ----------------------------------------------------------------------------

  /** Add a single document. Not visible to readers until [[commit]] runs.
   *
   *  Returns `Left` if a field in `doc` is missing from the schema or if the value type doesn't
   *  match the field's declared kind.
   */
  def index(doc: pb.Document): Either[String, Unit] =
    Ffi.call(pb.IndexDocumentRequest(indexId = id, document = Some(doc)), TantivyLib.indexDocument)(
      pb.GenericResponse.parseFrom
    ).flatMap(unitOrError)

  /** Bulk-add documents under a single writer-mutex acquisition. Faster than calling [[index]] in
   *  a loop, and atomic with respect to concurrent ops on the same index — either every doc in
   *  `docs` is queued for the next commit, or none are.
   */
  def indexBatch(docs: Seq[pb.Document]): Either[String, Unit] =
    Ffi.call(pb.IndexDocumentsRequest(indexId = id, documents = docs), TantivyLib.indexDocuments)(
      pb.GenericResponse.parseFrom
    ).flatMap(unitOrError)

  /** Insert-or-replace by id: deletes any existing doc whose id-field equals `idValue`, then adds
   *  `doc`. The schema must declare an `id_field` on creation (or `idFieldOverride` must be set).
   *
   *  Tantivy implements this as `delete_term` + `add_document` under the writer mutex; the two
   *  operations are atomic with respect to readers (only visible together after [[commit]]).
   */
  def upsert(idValue: pb.Value, doc: pb.Document, idFieldOverride: Option[String] = None): Either[String, Unit] =
    Ffi.call(
      pb.UpsertDocumentRequest(
        indexId = id,
        idFieldOverride = idFieldOverride,
        idValue = Some(idValue),
        document = Some(doc)
      ),
      TantivyLib.upsertDocument
    )(pb.GenericResponse.parseFrom).flatMap(unitOrError)

  /** Delete every doc whose id-field equals `idValue`. The schema must declare an `id_field`
   *  (or `idFieldOverride` must be set). Not visible to readers until [[commit]] runs.
   */
  def delete(idValue: pb.Value, idFieldOverride: Option[String] = None): Either[String, Unit] =
    Ffi.call(
      pb.DeleteRequest(indexId = id, idFieldOverride = idFieldOverride, idValue = Some(idValue)),
      TantivyLib.deleteDocument
    )(pb.GenericResponse.parseFrom).flatMap(unitOrError)

  /** Delete every doc matching `query`. Returns `0` for the deleted-estimate field today —
   *  Tantivy doesn't expose an exact count from this op. Not visible to readers until [[commit]]
   *  runs.
   */
  def deleteByQuery(query: pb.Query): Either[String, Long] =
    Ffi.call(pb.DeleteByQueryRequest(indexId = id, query = Some(query)), TantivyLib.deleteByQuery)(
      pb.DeleteByQueryResponse.parseFrom
    ).flatMap(r => r.error.toLeft(r.deletedEstimate.getOrElse(0L)))

  /** Delete every document in the index. Equivalent to a `deleteByQuery(All)` but uses
   *  Tantivy's dedicated `delete_all_documents` path. Not visible to readers until [[commit]].
   */
  def truncate(): Either[String, Unit] =
    Ffi.call(pb.TruncateRequest(indexId = id), TantivyLib.truncate)(pb.GenericResponse.parseFrom)
      .flatMap(unitOrError)

  /** Trigger Tantivy's merge policy by issuing a commit. The `maxSegments` parameter is recorded
   *  but currently ignored — Tantivy 0.26 has no public "merge to N segments now" API.
   */
  def optimize(maxSegments: Int = 1): Either[String, Unit] =
    Ffi.call(pb.OptimizeRequest(indexId = id, maxSegments = maxSegments), TantivyLib.optimize)(
      pb.GenericResponse.parseFrom
    ).flatMap(unitOrError)

  /** Make all queued writes visible to readers and persist them on disk (for on-disk indexes).
   *  Returns Tantivy's monotonic opstamp. Until this is called, queued writes are invisible to
   *  search/count/aggregate/distinct.
   */
  def commit(): Either[String, Long] =
    Ffi.call(pb.CommitRequest(indexId = id), TantivyLib.commit)(pb.CommitResponse.parseFrom)
      .flatMap(r => r.error.toLeft(r.opstamp.getOrElse(0L)))

  /** Discard every uncommitted write since the last [[commit]]. Already-committed writes are
   *  unaffected.
   */
  def rollback(): Either[String, Unit] =
    Ffi.call(pb.RollbackRequest(indexId = id), TantivyLib.rollback)(pb.GenericResponse.parseFrom)
      .flatMap(unitOrError)

  // ---- read ops -----------------------------------------------------------------------------

  /** Count matching documents without materializing any. Cheaper than [[search]] with a high
   *  limit when you only need the cardinality.
   */
  def count(query: pb.Query): Either[String, Long] =
    Ffi.call(pb.CountRequest(indexId = id, query = Some(query)), TantivyLib.count)(pb.CountResponse.parseFrom)
      .flatMap(r => r.error.toLeft(r.count.getOrElse(0L)))

  /** Run a search with paging, sorting, faceting, and snippet generation as configured on
   *  `request`. The response carries hits in the requested conversion mode (full doc, single
   *  value, JSON, or materialized subset), optional total count, optional facet results, and
   *  one [[pb.SnippetResult]] per requested snippet spec per hit.
   *
   *  The request's `indexId` is auto-filled from this handle if left empty.
   */
  def search(request: pb.SearchRequest): Either[String, pb.SearchResponse] = {
    val withId = if request.indexId.isEmpty then request.copy(indexId = id) else request
    Ffi.call(withId, TantivyLib.search)(pb.SearchResponse.parseFrom)
      .flatMap(r => r.error match {
        case Some(e) => Left(e)
        case None => Right(r)
      })
  }

  /** Run an aggregation pass. Supports metric aggs (Sum / Avg / Min / Max / Count / Stats /
   *  Cardinality / Percentiles / Concat) and bucket aggs (Histogram / DateHistogram / Range /
   *  Terms), with sub-aggregations attachable to any bucket type.
   *
   *  The request's `indexId` is auto-filled from this handle if left empty.
   */
  def aggregate(request: pb.AggregationRequest): Either[String, pb.AggregationResponse] = {
    val withId = if request.indexId.isEmpty then request.copy(indexId = id) else request
    Ffi.call(withId, TantivyLib.aggregate)(pb.AggregationResponse.parseFrom)
      .flatMap(r => r.error match {
        case Some(e) => Left(e)
        case None => Right(r)
      })
  }

  /** Single page of distinct field values, sorted ascending. Use [[pb.DistinctResponse.nextCursor]]
   *  to fetch the next page; pass it back as `request.cursor`. The cursor is exclusive — the
   *  next page starts strictly after the last value of the previous page.
   */
  def distinctPage(request: pb.DistinctRequest): Either[String, pb.DistinctResponse] = {
    val withId = if request.indexId.isEmpty then request.copy(indexId = id) else request
    Ffi.call(withId, TantivyLib.distinct)(pb.DistinctResponse.parseFrom)
      .flatMap(r => r.error match {
        case Some(e) => Left(e)
        case None => Right(r)
      })
  }

  /** Tantivy's pretty-JSON `Explanation` for a single doc/query pair, useful for debugging
   *  relevance scores. Returns the empty string if no doc matches `sourceId`.
   *
   *  Requires the index to have been created (or reopened) with an `id_field` — that's how the
   *  source doc is resolved.
   */
  def explain(query: pb.Query, sourceId: String): Either[String, String] = {
    val req = pb.ExplainRequest(indexId = id, query = Some(query), sourceId = sourceId)
    Ffi.call(req, TantivyLib.explain)(pb.ExplainResponse.parseFrom).flatMap(r =>
      r.error match {
        case Some(e) => Left(e)
        case None    => Right(r.explanation)
      }
    )
  }

  /** Drop this handle from the Rust-side registry. Best-effort commits any pending writes (an
   *  in-flight transaction is finalized; an outright failure to commit is silently swallowed).
   *  Idempotent.
   */
  override def close(): Unit = {
    val req = pb.DropIndexRequest(indexId = id)
    Ffi.call(req, TantivyLib.dropIndex)(pb.GenericResponse.parseFrom)
    ()
  }

  private def unitOrError(r: pb.GenericResponse): Either[String, Unit] =
    r.error match {
      case Some(e) => Left(e)
      case None => Right(())
    }
}
