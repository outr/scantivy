package scantivy

import java.io.{File, FileOutputStream, InputStream}
import java.lang.foreign.{Arena, SymbolLookup}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

/** Detects the running OS+arch, locates the native library, and exposes an FFM [[SymbolLookup]].
 *
 *  Layout in the published jar:
 *  {{{
 *    /native/linux-x86_64/libscantivy.so
 *    /native/linux-aarch64/libscantivy.so
 *    /native/macos-aarch64/libscantivy.dylib
 *    /native/windows-x86_64/scantivy.dll
 *  }}}
 *
 *  When the resource is found in the jar, the bytes are extracted to
 *  `${tmpdir}/scantivy-<sha256>/<libname>` (sha-keyed so re-published bytes get a fresh path) and
 *  loaded from there. When no resource is bundled, falls back to scanning `java.library.path` so
 *  local dev with `scala/lib/libscantivy.<ext>` keeps working.
 */
object NativeLibLoader {
  @volatile private var resolved: Option[Path] = None
  private val lock = new Object

  /** Resolve the native library to an absolute path, performing the resource extraction on the
   *  first call. Idempotent and thread-safe.
   */
  def ensurePath(): Path = resolved match {
    case Some(p) => p
    case None =>
      lock.synchronized {
        resolved.getOrElse {
          val (osArch, libFile) = detectPlatform()
          val resourcePath = s"/native/$osArch/$libFile"
          val p = readResource(resourcePath) match {
            case Some(bytes) => extractTo(libFile, bytes)
            case None        => searchLibraryPath(libFile)
          }
          resolved = Some(p)
          p
        }
      }
  }

  /** FFM symbol lookup for the loaded library. Scoped to the global arena so the lib outlives the
   *  caller; closing this arena would invalidate every [[java.lang.invoke.MethodHandle]] linked
   *  through it, so we deliberately do not expose a closable arena.
   */
  lazy val symbolLookup: SymbolLookup =
    SymbolLookup.libraryLookup(ensurePath(), Arena.global())

  private def detectPlatform(): (String, String) = {
    val osName = System.getProperty("os.name").toLowerCase
    val arch = System.getProperty("os.arch").toLowerCase
    val (os, libname) =
      if osName.contains("linux") then ("linux", "libscantivy.so")
      else if osName.contains("mac") || osName.contains("darwin") then ("macos", "libscantivy.dylib")
      else if osName.contains("win") then ("windows", "scantivy.dll")
      else throw new UnsupportedOperationException(s"Unsupported OS: $osName")
    val a =
      if arch == "amd64" || arch == "x86_64" then "x86_64"
      else if arch == "aarch64" || arch == "arm64" then "aarch64"
      else throw new UnsupportedOperationException(s"Unsupported arch: $arch")
    (s"$os-$a", libname)
  }

  private def readResource(path: String): Option[Array[Byte]] = {
    val in: InputStream = getClass.getResourceAsStream(path)
    if in == null then None
    else try Some(in.readAllBytes()) finally in.close()
  }

  private def extractTo(libFile: String, bytes: Array[Byte]): Path = {
    val sha = sha256(bytes)
    val targetDir = Paths.get(System.getProperty("java.io.tmpdir"), s"scantivy-$sha")
    Files.createDirectories(targetDir)
    val target = targetDir.resolve(libFile)
    if !Files.exists(target) then {
      val out = new FileOutputStream(target.toFile)
      try out.write(bytes) finally out.close()
      target.toFile.setExecutable(true)
    }
    target.toAbsolutePath
  }

  private def searchLibraryPath(libFile: String): Path = {
    val raw = Option(System.getProperty("java.library.path")).getOrElse("")
    val candidates = raw.split(File.pathSeparator).iterator
      .filter(_.nonEmpty)
      .map(p => Paths.get(p, libFile))
    candidates
      .find(Files.exists(_))
      .map(_.toAbsolutePath)
      .getOrElse {
        throw new UnsupportedOperationException(
          s"Native library '$libFile' not found in jar resources or java.library.path. " +
          s"For local dev: build with `cargo build --release` and copy " +
          s"rust/target/release/$libFile into scala/lib/."
        )
      }
  }

  private def sha256(bytes: Array[Byte]): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(bytes)
    md.digest().take(8).map(b => "%02x".format(b & 0xff)).mkString
  }
}
