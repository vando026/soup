package conviva.soup

class ClustDesignSuite extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  // import conviva.soup.Design.Clust2Stage

  val path = "./soup/src/test/data"

  val spark = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  val dat = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(path + "/schools.csv")

  // // cluster with math
  // val maxmath = dat.select(max("math")).first.getInt(0).toDouble
  // val data = dat 
  //   .withColumn("pmath", col("math")/lit(maxmath))
  //   .groupBy("schoolid")
  //   .agg(
  //     first("Mi").alias("N"),
  //     mean("math").alias("ybar"),
  //     mean("pmath").alias("pybar"),
  //     variance("math").alias("ys2"),
  //     sum("math").alias("ytot")
  //   )
  //   .withColumn("smpN", lit(20))



  // test("Cluster 2 stage schools should be") {
  //   val clust = Clust2Stage(data)
  //   val t1 = clust.svymean()
  // }

  // cluster with percentage math

  // test("Cluster 2 stage schools prop should be") {
  //   val pclust = Clust2Stage(data)
  //   val t1 = pclust.svymean()
  // }


}
