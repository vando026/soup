package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
// import breeze.linalg._
// import breeze.numerics._
import conviva.soup.Design.{Simple, Stratified}

class DataSuite extends munit.FunSuite {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val R = org.ddahl.rscala.RClient()

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
    val acres92 = dat.select(col("acres92"))
      .collect.map(_.getInt(0).toDouble)
    val lt200k = dat.select(col("lt200k"))
      .collect.map(_.getInt(0).toDouble)
    val fpc = Array.fill(n)(N)
    val pweights = Array.fill(n)(N/n)

    // val dacres92 = DenseVector(acres92)
    // val dlt200k = DenseVector(lt200k)
    // val id  = List.range(1, n + 1)
    // val dacres92 = DenseVector(acres92)
    // val dlt200k = DenseVector(lt200k)
    // val strata = DenseVector.ones[Int](n)
    // val pweights = DenseVector.fill(n){ N/n.toDouble }
    // val prob = 1.0 / pweights
    // val psum = sum(pweights)
    // val ave = sum(x * pweights/psum)


    test("Agsrs means should be expected") {
      val dsrs = Simple(acres92, weights = pweights, fpc = fpc )
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
      val ltsrs = Simple(lt200k, weights = pweights, fpc = fpc)
      val t1 = ltsrs.estimate("svymean")
      val t2 = ltsrs.confint("svymean")
      assertEquals(t1(0), 0.51)
      assertEquals(t1(1), 0.0275)
      assertEquals((t2(0) * 100).round.toDouble/100, 0.46)
      assertEquals((t2(1) * 100).round.toDouble/100, 0.56)
    }

    val strdat = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agstrat.csv")
      .withColumn("lt200k",
        when(col("acres92") < 2e5, 1).otherwise(0))
      .withColumn("popsize", 
        when(col("region") === "NE", 220)
        .when(col("region") === "NC", 1053)
        .when(col("region") === "S", 1382)
        .when(col("region") === "W", 422))
    strdat.groupBy("popsize").agg(count("*")).show()

    val strata = strdat.select(col("region"))
      .collect.map(_.getString(0))
    val stfpc = strdat.select(col("popsize"))
      .collect.map(_.getInt(0).toDouble)
    val stpweights = strdat.select(col("strwt"))
      .collect.map(_.getDouble(0))
    val y = strdat.select(col("acres92"))
      .collect.map(_.getInt(0).toDouble)

    test("Agstr mean and total should be expected") {
      val dstr = Stratified(y, strata, stpweights, stfpc)
      val t0 = dstr.estimate("svymean").map(_.toInt)
      val t1 = dstr.confint("svymean").map(_.toInt)
      val t2 = dstr.estimate("svytotal").map(_.toInt)
      val t3 = dstr.confint("svytotal").map(_.toInt)
      assertEquals(t0(0), 295560)
      assertEquals(t0(1), 16379)
      assertEquals(t1(0), 263326)
      assertEquals(t1(1), 327795)
      assertEquals(t2(0), 909736035)
      assertEquals(t2(1), 50416954)
      assertEquals(t3(0), 810519015)
      assertEquals(t3(1), 1008953055)
    }

    test("Agstr props should be expected") {
      val ltstr = Stratified(lt200k, strata, stpweights, fpc)
      val t1 = ltstr.estimate("svymean")
      val t2 = ltstr.confint("svymean")
      assertEquals(t1(0), 0.5103)
      assertEquals(t1(1), 0.0282)
      assertEquals((t2(0) * 1000).round.toDouble/1000, 0.455)
      assertEquals((t2(1) * 1000).round.toDouble/1000, 0.566)
    }


}

// spark-shell --jars /Users/avandormael/Documents/ConvivaRepos/sampling/soup/target/scala-2.12/soup_2.12-0.0.1.jar,/Users/avandormael/Documents/ConvivaRepos/surgeon/surgeon/target/scala-2.12/surgeon_2.12-0.0.2.jar,/Users/avandormael/Workspace/jars/rscala_2.12-3.2.19.jar
