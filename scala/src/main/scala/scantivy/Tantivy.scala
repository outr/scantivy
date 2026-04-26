package scantivy

import _root_.scantivy.proto as pb
import java.lang.foreign.{Arena, MemorySegment, ValueLayout}
import java.lang.invoke.MethodHandle
import scalapb.GeneratedMessage
import scala.util.Using

/** Entry point for creating and reattaching to Tantivy indexes.
 *
 *  Two ways to construct an index:
 *  - [[create]] — build a new index from a [[pb.CreateIndexRequest]]. If `request.path` is set,
 *    the index is persisted on disk and can be reopened later with [[open]]; otherwise it lives
 *    in memory only and disappears with the JVM.
 *  - [[open]] — reattach to an existing on-disk index produced by an earlier `create`.
 *
 *  Both return `Either[String, TantivyIndex]`. The handle is thread-safe and shareable; close it
 *  via [[TantivyIndex.close]] when done so the Rust-side registry can drop it.
 */
object Tantivy {
  // Force the loader to extract / locate the lib eagerly so that link errors surface here rather
  // than on the first call to a downstream op.
  private val _lookup = TantivyLib.freeBuffer

  /** Create a new index. If `request.path` is set, the index is persisted on disk; otherwise it
   *  lives in memory only.
   *
   *  Returns `Left` for schema errors (missing field name, unsupported field kind), IO errors
   *  (couldn't create the on-disk directory, couldn't open MmapDirectory), or Tantivy-internal
   *  failures during writer/reader construction.
   */
  def create(request: pb.CreateIndexRequest): Either[String, TantivyIndex] = {
    val resp = Ffi.call(request, TantivyLib.createIndex)(pb.CreateIndexResponse.parseFrom)
    resp.flatMap(r => r.error match {
      case Some(e) => Left(e)
      case None => r.indexId match {
        case Some(id) => Right(new TantivyIndex(id))
        case None => Left("create_index: response missing index_id")
      }
    })
  }

  /** Reattach to an on-disk index.
   *
   *  `idField` re-establishes the schema's id field — Tantivy's on-disk format does not persist it,
   *  so reopening without it disables `upsert` / `delete(idValue)` / `MoreLikeThis` / `explain`.
   *  Leave unset for read-only / search-only usage.
   */
  def open(path: String, idField: Option[String] = None): Either[String, TantivyIndex] = {
    val req = pb.OpenIndexRequest(path = path, idField = idField)
    val resp = Ffi.call(req, TantivyLib.openIndex)(pb.CreateIndexResponse.parseFrom)
    resp.flatMap(r => r.error match {
      case Some(e) => Left(e)
      case None => r.indexId match {
        case Some(id) => Right(new TantivyIndex(id))
        case None => Left("open_index: response missing index_id")
      }
    })
  }
}

/** Helpers for shipping proto bytes across the FFI boundary using FFM (JEP 442). */
private[scantivy] object Ffi {
  /** Encode `req`, call the FFI op via `mh`, decode the response into `T`. The native lib returns
   *  a heap pointer + length; we copy the bytes into a JVM `Array[Byte]` and immediately call
   *  `free_buffer` so the Rust side reclaims its heap.
   */
  def call[Req <: GeneratedMessage, T](req: Req, mh: MethodHandle)(parse: Array[Byte] => T): Either[String, T] = {
    val bytes = req.toByteArray
    Using.resource(Arena.ofConfined()) { arena =>
      val reqLen = bytes.length
      val reqSeg: MemorySegment = arena.allocate(math.max(reqLen, 1).toLong)
      if reqLen > 0 then {
        MemorySegment.copy(bytes, 0, reqSeg, ValueLayout.JAVA_BYTE, 0L, reqLen)
      }
      val outLenSeg: MemorySegment = arena.allocate(ValueLayout.JAVA_LONG)

      // Scala 3 only emits the polymorphic-signature call descriptor when there is a direct type
      // ascription on the `invokeExact` result; `.asInstanceOf[...]` happens after the call and
      // leaves the descriptor as `Object`, which fails MethodHandle's exact-type check.
      val rawResp: MemorySegment = mh.invokeExact(reqSeg, reqLen.toLong, outLenSeg)
      val respLen = outLenSeg.get(ValueLayout.JAVA_LONG, 0L)

      val out = new Array[Byte](respLen.toInt)
      if respLen > 0 then {
        // The downcall returns a zero-length address segment; reinterpret to give it a known size
        // before copying out.
        val sized = rawResp.reinterpret(respLen)
        MemorySegment.copy(sized, ValueLayout.JAVA_BYTE, 0L, out, 0, respLen.toInt)
      }
      val _unit: Unit = TantivyLib.freeBuffer.invokeExact(rawResp, respLen)

      try Right(parse(out))
      catch case t: Throwable => Left(s"failed to decode response: ${t.getMessage}")
    }
  }
}
