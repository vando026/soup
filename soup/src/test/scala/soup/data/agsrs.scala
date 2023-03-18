package sampling

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import breeze.linalg._
import breeze.numerics._

class DataSuite extends munit.FunSuite {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val path = "./src/test/scala/s4/data"
    val dat = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agsrs.csv")

      val n = dat.count.toInt
      val N = 3078
      val x = dat.select(col("acres92")).collect.map(_.getInt(0).toDouble)
      val dx = DenseVector(x)

      val id  = List.range(1, n + 1)
      val strata = DenseVector.ones[Int](n)
      val pweights = DenseVector.fill(n){ N/n.toDouble }
      val prob = 1.0 / pweights
      val psum = sum(pweights)
      val ave = sum(x * pweights/psum)

      val R = org.ddahl.rscala.RClient()
      val rq = "library(survey); svymean(%-)"
      



}
