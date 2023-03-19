package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
// import breeze.linalg._
// import breeze.numerics._
import conviva.soup.SRS._

class DataSuite extends munit.FunSuite {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val path = "./src/test/data"

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

    // val id  = List.range(1, n + 1)
    // val strata = DenseVector.ones[Int](n)
    // val pweights = DenseVector.fill(n){ N/n.toDouble }
    // val prob = 1.0 / pweights
    // val psum = sum(pweights)
    // val ave = sum(x * pweights/psum)

    val R = org.ddahl.rscala.RClient()

    test("Agsrs means should be expected") {
      val dsrs = svydesign(acres92, weights = pweights, fpc = fpc )
      val t0 = dsrs.estimate("svymean").map(_.toInt)
      val t1 = dsrs.confint("svymean").map(_.toInt)
      val t2 = dsrs.estimate("svytotal").map(_.toInt)
      val t3 = dsrs.confint("svytotal").map(_.toInt)
      assertEquals(t0(0), 297897)
      assertEquals(t0(1), 18898)
      assertEquals(t1(0), 260706)
      assertEquals(t1(1), 335087)
      assertEquals(t2(0), 916927109)
      assertEquals(t2(1), 58169381)
      assertEquals(t3(0), 802453858)
      assertEquals(t3(1), 1031400360)
    }

    test("Agsrs props should be expected") {
      val ltsrs = svydesign(lt200k, weights = pweights, fpc = fpc)
      val t1 = ltsrs.estimate("svymean")
      val t2 = ltsrs.confint("svymean")
      assertEquals(t1(0), 0.51)
      assertEquals(t1(1), 0.0275)
      assertEquals((t2(0) * 100).round.toDouble/100, 0.46)
      assertEquals((t2(1) * 100).round.toDouble/100, 0.56)
    }

  val dpath = "./src/test/data/pbssHourlyTest-c000.snappy.parquet"
  val popData = spark.read.parquet(dpath)
  val N = popData.count.toDouble // 10409
  popData.select(
    sum(col("justEnded")), avg(col("justEnded")),
    sum(col("joinTimeSec")), avg(col("joinTimeSec")))
      .show()
  // 9544 and 18098.669

  val sampData = popData.sample(0.2, 900600)
  val jTimeSec = sampData.select("joinTimeSec")
    .collect.map(_.getDouble(0))
  val justEnded = sampData.select("justEnded")
    .collect.map(_.getInt(0).toDouble)
  val n = sampData.count.toInt
  val pweights = Array.fill(n)(N/n)
  val fpc = Array.fill(n)(N)
  val dsrs = svydesign(jTimeSec, weights = pweights, fpc = fpc )
  dsrs.estimate("svymean")
  dsrs.estimate("svytotal")
  dsrs.confint("svytotal")
  dsrs.confint("svymean")
  val lsrs = svydesign(justEnded, weights = pweights, fpc = fpc )
  lsrs.estimate("svymean")
  lsrs.confint("svymean")
  lsrs.estimate("svytotal")
}

// spark-shell --jars /Users/avandormael/Documents/ConvivaRepos/sampling/soup/target/scala-2.12/soup_2.12-0.0.1.jar,/Users/avandormael/Documents/ConvivaRepos/surgeon/surgeon/target/scala-2.12/surgeon_2.12-0.0.2.jar,/Users/avandormael/Workspace/jars/rscala_2.12-3.2.19.jar
