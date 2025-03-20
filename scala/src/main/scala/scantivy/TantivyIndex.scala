package scantivy

import jnr.ffi.{LibraryLoader, Pointer}

import java.io.{File, FileOutputStream, IOException, InputStream}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

trait TantivyLib {
  def create_index(path: String): Pointer
  def add_document(index: Pointer, title: String, category: String): Pointer
  def search(index: Pointer, query: String, facet: String): Pointer
  def free_string(ptr: Pointer): Unit
}

object TantivyIndex {
  NativeLibLoader.extractLibrary() // ✅ Extract and load native library automatically
  private val tantivy = LibraryLoader.create(classOf[TantivyLib]).load("scantivy")

  def create(path: Option[Path] = None): TantivyIndex = {
    path.foreach { p =>
      Files.createDirectories(p)
    }
    val indexPtr = path match {
      case Some(path) => tantivy.create_index(path.toAbsolutePath.toString)
      case None => tantivy.create_index(null) // In-memory index
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

object TantivyExample {
  def main(args: Array[String]): Unit = {
    val dir = Path.of("test_index")
    // Create an index (in-memory or on-disk)
    val index = TantivyIndex.create(Some(dir)) // Use None for in-memory

    // Add documents with hierarchical facets
    index.addDocument("iPhone 13", FacetTree(List("electronics", "phones")))
    index.addDocument("MacBook Pro", FacetTree(List("electronics", "laptops")))
    index.addDocument("Samsung Galaxy S22", FacetTree(List("electronics", "phones")))

    // Search with only a text query
    val results1 = index.search(Some("iPhone"), None)
    println(s"Search Results (Text only):\n${results1.mkString("\n")}")

    // Search with only a facet filter
    val results2 = index.search(None, Some(FacetTree(List("electronics", "phones"))))
    println(s"Search Results (Facet only):\n${results2.mkString("\n")}")

    // Search with both a text query and a facet filter
    val results3 = index.search(Some("iPhone"), Some(FacetTree(List("electronics", "phones"))))
    println(s"Search Results (Text + Facet):\n${results3.mkString("\n")}")

    // Search with neither (returns all documents)
    val results4 = index.search()
    println(s"All Documents:\n${results4.mkString("\n")}")

    deleteDirectory(dir)
  }

  def deleteDirectory(dir: Path): Unit = {
    if (Files.exists(dir)) {
      Files.walkFileTree(dir, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          super.visitFile(file, attrs)
        }
        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          super.postVisitDirectory(dir, exc)
        }
      })
    }
  }
}

object NativeLibLoader {
  def extractLibrary(): Unit = {
    val libName = System.getProperty("os.name").toLowerCase match {
      case os if os.contains("win") => "scantivy.dll"
      case os if os.contains("mac") => System.getProperty("os.arch") match {
        case arch if arch.contains("aarch64") || arch.contains("arm64") => "libscantivy-aarch64.dylib"
        case _ => "libscantivy-x86_64.dylib"
      }
      case _ => "libscantivy.so"
    }

    val tempFile = new File(System.getProperty("java.io.tmpdir"), libName)
    if (!tempFile.exists()) {
      val in: InputStream = getClass.getResourceAsStream(s"/native/$libName")
      if (in == null) throw new RuntimeException(s"Native library $libName not found in resources")

      val out = new FileOutputStream(tempFile)
      val buffer = new Array[Byte](1024)
      var bytesRead = in.read(buffer)
      while (bytesRead != -1) {
        out.write(buffer, 0, bytesRead)
        bytesRead = in.read(buffer)
      }
      in.close()
      out.close()
      tempFile.setExecutable(true)
    }

    System.load(tempFile.getAbsolutePath) // Load the extracted native library
  }
}