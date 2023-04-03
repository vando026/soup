package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.soup.Design.{Stratified}
import conviva.soup.Compute._

class StratDesignSuit extends munit.FunSuite {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    val path = "./src/test/data"

    val strdat_ = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agstrat.csv")
      .withColumn("lt200k",
        when(col("acres92") < 2e5, 1).otherwise(0))
      .withColumn("N_", 
        when(col("region") === "NE", 220.0)
        .when(col("region") === "NC", 1054.0)
        .when(col("region") === "S", 1382.0)
        .when(col("region") === "W", 422.0))


    def ex(x: String, d: DataFrame): Int = {
      math.round(d.select(col(x)).collect()(0).getDouble(0)).toInt
    }

    test("Agstr mean and total should be expected") {
      val strdat = Summarize(strdat_, col("acres92"), col("region")).data
      val dstr = Stratified(strdat)
      val t0 = dstr.svymean()
      val t1 = dstr.svytotal()
      assertEquals(ex("yest", t0), 295561)
      assertEquals(ex("yse", t0), 16380)
      assertEquals(ex("lb", t0), 263325)
      assertEquals(ex("ub", t0), 327797)
      assertEquals(ex("yest", t1), 909736035)
      assertEquals(ex("yse", t1), 50417248)
      assertEquals(ex("lb", t1), 810514350)
      assertEquals(ex("ub", t1), 1008957721)
    }

    def exd(x: String, d: DataFrame): Double = {
      d.select(col(x)).collect()(0).getDouble(0)
    }

    test("Agstr props should be expected") {
      val strdat = Summarize(strdat_, col("lt200k"), col("region")).data
      val ltstr = Stratified(strdat)
      val t1 = ltstr.svymean()
      assertEquals((exd("yest", t1) * 10000).round.toDouble/10000, 0.5139)
      assertEquals((exd("yse", t1) * 10000).round.toDouble/10000, 0.0248)
      assertEquals((exd("lb", t1) * 1000).round.toDouble/1000, 0.465)
      assertEquals((exd("ub", t1) * 1000).round.toDouble/1000, 0.563)
    }


}

