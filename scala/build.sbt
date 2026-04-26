name := "scantivy"
organization := "com.outr"
version := "1.0.0-SNAPSHOT"

scalaVersion := "3.8.3"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Wconf:any:silent")

// Scantivy uses the Foreign Function & Memory API (JEP 442, finalized in JDK 22) instead of
// JNR-FFI. Bumping the floor to 22 lets us drop the jnr-ffi dependency and use Arena/MemorySegment
// directly.
lazy val javaRelease: String = "22"

lazy val scalaTestVersion: String = "3.2.20"

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)

javacOptions ++= Seq("--release", javaRelease)
scalacOptions ++= Seq("-java-output-version", javaRelease)

fork := true
Test / fork := true

javaOptions ++= Seq("-Dfile.encoding=UTF-8")

// Detect the local OS+arch and stage the native lib under /native/<os>-<arch>/<libname> inside the
// produced jar. The CI release pipeline (Phase F) will run this build on each target platform and
// merge the produced libs into a single multi-arch jar before publishing to Maven Central.
lazy val nativeOs: String = sys.props.getOrElse("os.name", "").toLowerCase match {
  case s if s.contains("linux")  => "linux"
  case s if s.contains("mac")    => "macos"
  case s if s.contains("darwin") => "macos"
  case s if s.contains("win")    => "windows"
  case other                     => sys.error(s"unsupported os: $other")
}
lazy val nativeArch: String = sys.props.getOrElse("os.arch", "").toLowerCase match {
  case "amd64" | "x86_64"   => "x86_64"
  case "aarch64" | "arm64" => "aarch64"
  case other                => sys.error(s"unsupported arch: $other")
}
lazy val nativeLibName: String = nativeOs match {
  case "linux"   => "libscantivy.so"
  case "macos"   => "libscantivy.dylib"
  case "windows" => "scantivy.dll"
  case other     => sys.error(s"unsupported os: $other")
}

// Local-dev convenience: copy lib/libscantivy.* into /native/<os>-<arch>/ inside the jar so the
// loader finds it. CI release builds skip this and instead drop pre-built libs for ALL targets
// directly into src/main/resources/native/<os>-<arch>/ before packaging.
Compile / resourceGenerators += Def.task {
  val src = baseDirectory.value / "lib" / nativeLibName
  if (src.exists()) {
    val outDir = (Compile / resourceManaged).value / "native" / s"$nativeOs-$nativeArch"
    outDir.mkdirs()
    val dest = outDir / nativeLibName
    IO.copyFile(src, dest, preserveLastModified = true)
    Seq(dest)
  } else Seq.empty
}.taskValue

// Make sure the loader can find the lib during local sbt runs (sbt test). Adds java.library.path
// pointing at scala/lib for `NativeLibLoader`'s `searchLibraryPath` fallback.
Test / javaOptions += s"-Djava.library.path=${(baseDirectory.value / "lib").getAbsolutePath}"

// FFM marks `SymbolLookup.libraryLookup` and downcall handles as restricted; opt in once globally
// for the unnamed module so the JVM stops emitting "WARNING: A restricted method..." noise. Library
// consumers who run scantivy from a named module should pass `--enable-native-access=<module>`
// themselves; for classpath/unnamed-module consumers, ALL-UNNAMED is the right scope.
Test / javaOptions += "--enable-native-access=ALL-UNNAMED"

// ----- Sonatype / Maven Central publishing -----------------------------------------------------

publishMavenStyle := true
Test / publishArtifact := false
publishTo := sonatypePublishToBundle.value
sonatypeCredentialHost := "central.sonatype.com"
sonatypeProfileName := "com.outr"

licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))
homepage := Some(url("https://github.com/outr/scantivy"))
scmInfo := Some(ScmInfo(
  url("https://github.com/outr/scantivy"),
  "scm:git@github.com:outr/scantivy.git"
))
developers := List(
  Developer(id = "darkfrog", name = "Matt Hicks", email = "matt@outr.com", url = url("https://matthicks.com"))
)
