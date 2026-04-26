package scantivy

import _root_.scantivy.proto as pb

/** Handle to an open Tantivy index. Cheap to clone; thread-safe — the underlying Rust handle is
 *  guarded by a mutex on the writer and an auto-refreshing reader.
 *
 *  Lifecycle:
 *  - Created by [[Tantivy.create]] / [[Tantivy.open]].
 *  - Closed via [[close]] (also runs on JVM shutdown via the registry's drop_index FFI).
 */
final class TantivyIndex private[scantivy] (val id: String) extends AutoCloseable {

  // ---- write ops ----------------------------------------------------------------------------

  def index(doc: pb.Document): Either[String, Unit] =
    Ffi.call(pb.IndexDocumentRequest(indexId = id, document = Some(doc)), TantivyLib.indexDocument)(
      pb.GenericResponse.parseFrom
    ).flatMap(unitOrError)

  def indexBatch(docs: Seq[pb.Document]): Either[String, Unit] =
    Ffi.call(pb.IndexDocumentsRequest(indexId = id, documents = docs), TantivyLib.indexDocuments)(
      pb.GenericResponse.parseFrom
    ).flatMap(unitOrError)

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

  def delete(idValue: pb.Value, idFieldOverride: Option[String] = None): Either[String, Unit] =
    Ffi.call(
      pb.DeleteRequest(indexId = id, idFieldOverride = idFieldOverride, idValue = Some(idValue)),
      TantivyLib.deleteDocument
    )(pb.GenericResponse.parseFrom).flatMap(unitOrError)

  def deleteByQuery(query: pb.Query): Either[String, Long] =
    Ffi.call(pb.DeleteByQueryRequest(indexId = id, query = Some(query)), TantivyLib.deleteByQuery)(
      pb.DeleteByQueryResponse.parseFrom
    ).flatMap(r => r.error.toLeft(r.deletedEstimate.getOrElse(0L)))

  def truncate(): Either[String, Unit] =
    Ffi.call(pb.TruncateRequest(indexId = id), TantivyLib.truncate)(pb.GenericResponse.parseFrom)
      .flatMap(unitOrError)

  def optimize(maxSegments: Int = 1): Either[String, Unit] =
    Ffi.call(pb.OptimizeRequest(indexId = id, maxSegments = maxSegments), TantivyLib.optimize)(
      pb.GenericResponse.parseFrom
    ).flatMap(unitOrError)

  def commit(): Either[String, Long] =
    Ffi.call(pb.CommitRequest(indexId = id), TantivyLib.commit)(pb.CommitResponse.parseFrom)
      .flatMap(r => r.error.toLeft(r.opstamp.getOrElse(0L)))

  def rollback(): Either[String, Unit] =
    Ffi.call(pb.RollbackRequest(indexId = id), TantivyLib.rollback)(pb.GenericResponse.parseFrom)
      .flatMap(unitOrError)

  // ---- read ops -----------------------------------------------------------------------------

  def count(query: pb.Query): Either[String, Long] =
    Ffi.call(pb.CountRequest(indexId = id, query = Some(query)), TantivyLib.count)(pb.CountResponse.parseFrom)
      .flatMap(r => r.error.toLeft(r.count.getOrElse(0L)))

  def search(request: pb.SearchRequest): Either[String, pb.SearchResponse] = {
    val withId = if request.indexId.isEmpty then request.copy(indexId = id) else request
    Ffi.call(withId, TantivyLib.search)(pb.SearchResponse.parseFrom)
      .flatMap(r => r.error match {
        case Some(e) => Left(e)
        case None => Right(r)
      })
  }

  def aggregate(request: pb.AggregationRequest): Either[String, pb.AggregationResponse] = {
    val withId = if request.indexId.isEmpty then request.copy(indexId = id) else request
    Ffi.call(withId, TantivyLib.aggregate)(pb.AggregationResponse.parseFrom)
      .flatMap(r => r.error match {
        case Some(e) => Left(e)
        case None => Right(r)
      })
  }

  /** Single-page distinct call. Higher-level streaming should chain this until `nextCursor` is empty. */
  def distinctPage(request: pb.DistinctRequest): Either[String, pb.DistinctResponse] = {
    val withId = if request.indexId.isEmpty then request.copy(indexId = id) else request
    Ffi.call(withId, TantivyLib.distinct)(pb.DistinctResponse.parseFrom)
      .flatMap(r => r.error match {
        case Some(e) => Left(e)
        case None => Right(r)
      })
  }

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
