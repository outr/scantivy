addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
// Derive `version :=` from `git describe`, so the published version is automatically the git
// tag. Eliminates drift between build.sbt and the tag (the bug that silently published 1.0.0
// when v1.0.0-rc1 was tagged).
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.0.1")
