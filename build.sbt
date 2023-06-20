scalaVersion := "2.12.13"
version := "0.0.5"
name := "sampling"
organization := "conviva"

lazy val soup = (
  Project("soup", file("soup"))
   .settings(
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-sql" % "3.3.1",
      "org.scalameta" %% "munit" % "0.7.29" % Test,
     )
   )
)

logLevel := Level.Warn
publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// Global / semanticdbEnabled := true

lazy val docs = project
  .in(file("soup.wiki"))
  .dependsOn(soup)
  .enablePlugins(MdocPlugin)
