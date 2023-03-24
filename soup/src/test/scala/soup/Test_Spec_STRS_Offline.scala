package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.soup.Design.{Simple, Stratified}

class StratDesignSuit extends munit.FunSuite {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    val R = org.ddahl.rscala.RClient()

    val path = "./src/test/data"

    val sampData = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agsrs.csv")
      .select(
        col("acres92"),
        when(col("acres92") < 2e5, 1).otherwise(0).alias("lt200k")
      )

    val strdat = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agstrat.csv")
      .withColumn("lt200k",
        when(col("acres92") < 2e5, 1).otherwise(0))
      strdat.agg(mean("acres92"), stddev("acres92")).show

    val wdat = strdat.groupBy("region")
      .agg(count("*").cast("double").alias("strataSize"))
      .withColumn("popsize", 
        when(col("region") === "NE", 220.0)
        .when(col("region") === "NC", 1054.0)
        .when(col("region") === "S", 1382.0)
        .when(col("region") === "W", 422.0))
    
    val strdat1 = strdat.join(wdat, List("region"), "left")
      .withColumn("stweight", round(col("popSize")/col("strataSize"), 8))

    val strata = strdat1.select(col("region"))
      .collect.map(_.getString(0))
    val stfpc = strdat1.select(col("popSize"))
      .collect.map(_.getDouble(0))
    val stweights = strdat1.select(col("stweight"))
      .collect.map(_.getDouble(0))
    val acres = strdat1.select(col("acres92"))
      .collect.map(_.getInt(0).toDouble)
    val lt200k = strdat1.select(col("lt200k"))
      .collect.map(_.getInt(0).toDouble)

    test("Agstr mean and total should be expected") {
      val dstr = Stratified(acres, strata, stweights, stfpc)
      val t0 = dstr.estimate("svymean").map(_.toDouble)
      val t1 = dstr.confint("svymean").map(_.toDouble)
      val t2 = dstr.estimate("svytotal").map(_.toDouble)
      val t3 = dstr.confint("svytotal").map(_.toDouble)
      assertEquals(math.round(t0(0)), 295561L)
      assertEquals(math.round(t0(1)), 16380L)
      assertEquals(math.round(t1(0)), 263325L)
      assertEquals(math.round(t1(1)), 327797L)
      assertEquals(math.round(t2(0)), 909736036L)
      assertEquals(math.round(t2(1)), 50417248L)
      assertEquals(math.round(t3(0)), 810514350L)
      assertEquals(math.round(t3(1)), 1008957721L)
    }

    test("Agstr props should be expected") {
      val ltstr = Stratified(lt200k, strata, stweights, stfpc)
      val t1 = ltstr.estimate("svymean")
      val t2 = ltstr.confint("svymean")
      assertEquals(t1(0), 0.5139)
      assertEquals(t1(1), 0.0248)
      assertEquals((t2(0) * 1000).round.toDouble/1000, 0.465)
      assertEquals((t2(1) * 1000).round.toDouble/1000, 0.563)
    }


}

