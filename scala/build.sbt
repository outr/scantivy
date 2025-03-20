name := "scantivy"
organization := "com.outr"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.16"

libraryDependencies += "com.github.jnr" % "jnr-ffi" % "2.2.17"

javaOptions += s"-Djava.library.path=${(baseDirectory.value / "lib").getAbsolutePath}"

Compile / resourceDirectory := baseDirectory.value / "lib"
Compile / unmanagedResourceDirectories += baseDirectory.value / "lib"

Compile / packageBin / mappings ++= Def.setting {
  val libDir = (baseDirectory.value / "lib")
  val libFiles = (libDir ** "*scantivy*").get
  libFiles.map { f => f -> s"native/${f.getName}" }
}.value

fork := true