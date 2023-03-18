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

    val path = "./src/test/scala/soup/data"

    val dat = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agsrs.csv")
      .select(
        col("acres92"),
        when(col("acres92") < 2e5, 1).otherwise(0).alias("lt200k")
      )

      val n = dat.count.toInt
      val N: Double = 3078
      val dat1 = dat
      val acres92 = dat.select(col("acres92"))
        .collect.map(_.getInt(0).toDouble)
      val lt200k = dat.select(col("lt200k"))
        .collect.map(_.getInt(0).toDouble)
      val dacres92 = DenseVector(acres92)
      val dlt200k = DenseVector(lt200k)
      val fpc = Array.fill(n)(N)
      val pweights = Array.fill(n)(N/n)

      val id  = List.range(1, n + 1)
      val strata = DenseVector.ones[Int](n)
      val pweights = DenseVector.fill(n){ N/n.toDouble }
      val prob = 1.0 / pweights
      val psum = sum(pweights)
      val ave = sum(x * pweights/psum)

      val R = org.ddahl.rscala.RClient()

    case class svydesign(y: Array[Double], weights: Array[Double], fpc: Array[Double]) {
      val lib = "library(survey);"
      val df =  "dat <- data.frame(y = %-, sampwt = %-);"
      val design = "dsrs <- svydesign(id = ~1, weights = ~sampwt, fpc = %-, data = dat);"
      def calcEst(method: String): String =  s"res <- ${method}(~y, dsrs);"
      val cmd = s"$lib $df $design"
      def estimate(method: String = "svymean"): Array[Double] = {
        val result = "round(c(as.numeric(res[1]), sqrt(as.numeric(attributes(res)$var))), 4)"
        val rsnip = s"""
          | $cmd
          | ${calcEst(method)}
          | $result
        """.stripMargin
        R.evalD1(rsnip, y, weights, fpc)
      }
      def confint(method: String = "svymean"): Array[Double] = {
        val result = s"cc <- confint(res, df=%-); c(cc[1], cc[2])"
        val rsnip = s"""
          | $cmd
          | ${calcEst(method)}
          | $result
        """.stripMargin
        R.evalD1(rsnip, y, weights, fpc, y.length - 1)
      }
    }

    val dsrs = svydesign(acres92, weights = pweights, fpc = fpc )
    dsrs.estimate("svymean")
    dsrs.confint("svymean")
    dsrs.estimate("svytotal")
    dsrs.confint("svytotal")
    val ltsrs = svydesign(lt200k, weights = pweights, fpc = fpc)
    ltsrs.estimate("svymean")
    ltsrs.confint("svymean")

}

val tt = s"""
  | This is my ${66*1}
  | and so os this ${2 *2}
""".stripMargin
