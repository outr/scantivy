name := "scantivy"
organization := "com.outr"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.16"

libraryDependencies += "com.github.jnr" % "jnr-ffi" % "2.2.17"

javaOptions += "-Djava.library.path=lib"