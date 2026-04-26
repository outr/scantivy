package scantivy

import java.lang.foreign.{FunctionDescriptor, Linker, MemorySegment, ValueLayout}
import java.lang.invoke.MethodHandle

/** FFM-bound entry points for the Rust `libscantivy` shared library.
 *
 *  Every op shares the same C ABI: `extern "C" fn(req_ptr: *const u8, req_len: usize,
 *  out_len: *mut usize) -> *mut u8`. The returned pointer must be released via [[freeBuffer]] —
 *  see `Ffi.call` in [[Tantivy]] for the lifecycle.
 */
object TantivyLib {
  private val linker: Linker = Linker.nativeLinker()

  /** Shape shared by every proto-encoded op. `usize` is 64-bit on every platform we ship to, so
   *  `JAVA_LONG` is correct for both `req_len` and the `*mut usize` out-param.
   */
  private val opDesc: FunctionDescriptor = FunctionDescriptor.of(
    ValueLayout.ADDRESS,    // returns *mut u8
    ValueLayout.ADDRESS,    // req_ptr: *const u8
    ValueLayout.JAVA_LONG,  // req_len: usize
    ValueLayout.ADDRESS,    // out_len: *mut usize
  )

  private val freeDesc: FunctionDescriptor = FunctionDescriptor.ofVoid(
    ValueLayout.ADDRESS,    // ptr
    ValueLayout.JAVA_LONG,  // len
  )

  private def link(name: String, desc: FunctionDescriptor): MethodHandle = {
    val sym: MemorySegment = NativeLibLoader.symbolLookup
      .find(name)
      .orElseThrow(() => new UnsatisfiedLinkError(s"missing scantivy symbol: $name"))
    linker.downcallHandle(sym, desc)
  }

  // -- lifecycle ----------------------------------------------------------------------------
  val createIndex: MethodHandle = link("create_index", opDesc)
  val openIndex:   MethodHandle = link("open_index", opDesc)
  val dropIndex:   MethodHandle = link("drop_index", opDesc)

  // -- write ops ----------------------------------------------------------------------------
  val indexDocument:   MethodHandle = link("index_document", opDesc)
  val indexDocuments:  MethodHandle = link("index_documents", opDesc)
  val upsertDocument:  MethodHandle = link("upsert_document", opDesc)
  val deleteDocument:  MethodHandle = link("delete_document", opDesc)
  val deleteByQuery:   MethodHandle = link("delete_by_query", opDesc)
  val truncate:        MethodHandle = link("truncate", opDesc)
  val optimize:        MethodHandle = link("optimize", opDesc)
  val commit:          MethodHandle = link("commit", opDesc)
  val rollback:        MethodHandle = link("rollback", opDesc)

  // -- read ops -----------------------------------------------------------------------------
  val count:     MethodHandle = link("count", opDesc)
  val search:    MethodHandle = link("search", opDesc)
  val aggregate: MethodHandle = link("aggregate", opDesc)
  val distinct:  MethodHandle = link("distinct", opDesc)
  val explain:   MethodHandle = link("explain", opDesc)

  // -- buffer mgmt --------------------------------------------------------------------------
  val freeBuffer: MethodHandle = link("free_buffer", freeDesc)
}
