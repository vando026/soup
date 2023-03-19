scalaVersion := "2.12.13"
version := "0.0.1"
name := "soup"
organization := "conviva"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.ddahl" %% "rscala" % "3.2.19",
  "org.scalanlp" %% "breeze" % "1.1",
  "org.scalameta" %% "munit" % "0.7.29" % Test
)

publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
logLevel := Level.Warn

unmanagedBase := new java.io.File("/Users/avandormael/miniconda3/envs/dbconnect/lib/python3.9/site-packages/pyspark/jars")
