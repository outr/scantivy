package scantivy

import jnr.ffi.{LibraryLoader, Pointer}

trait TantivyLib {
  def create_index(path: String): Pointer
  def add_document(index: Pointer, title: String, category: String): Pointer
  def search(index: Pointer, query: String, facet: String): Pointer
  def free_string(ptr: Pointer): Unit
}

object TantivyIndex {
  private val tantivy = LibraryLoader.create(classOf[TantivyLib]).load("tantivy_wrapper")

  def create(optionalPath: Option[String] = None): TantivyIndex = {
    val indexPtr = optionalPath match {
      case Some(path) => tantivy.create_index(path)
      case None       => tantivy.create_index(null) // In-memory index
    }
    new TantivyIndex(indexPtr)
  }
}

class TantivyIndex(private val indexPtr: Pointer) {
  def addDocument(title: String, category: FacetTree): Unit = {
    val categoryString = category.toTantivyPath
    val resultPtr = TantivyIndex.tantivy.add_document(indexPtr, title, categoryString)
    TantivyIndex.tantivy.free_string(resultPtr)
  }

  def search(query: Option[String] = None, facet: Option[FacetTree] = None): Seq[String] = {
    val queryStr = query.orNull
    val facetStr = facet.map(_.toTantivyPath).orNull

    val resultPtr = TantivyIndex.tantivy.search(indexPtr, queryStr, facetStr)
    val results = resultPtr.getString(0).split("\n").toSeq
    TantivyIndex.tantivy.free_string(resultPtr)
    results
  }
}

case class FacetTree(levels: List[String]) {
  def toTantivyPath: String = "/" + levels.mkString("/")
}
