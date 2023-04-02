package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.soup.Design.{Stratified}

class StratDesignSuit extends munit.FunSuite {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

//     val R = org.ddahl.rscala.RClient()

    val path = "./src/test/data"

    val strdat = spark.read
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

//     val strata = strdat1.select(col("region"))
//       .collect.map(_.getString(0))
//     val stfpc = strdat1.select(col("popSize"))
//       .collect.map(_.getDouble(0))
//     val stweights = strdat1.select(col("stweight"))
//       .collect.map(_.getDouble(0))
//     val acres = strdat1.select(col("acres92"))
//       .collect.map(_.getInt(0).toDouble)
//     val lt200k = strdat1.select(col("lt200k"))
//       .collect.map(_.getInt(0).toDouble)

    def ex(x: String, d: DataFrame): Int = {
      math.round(d.select(col(x)).collect()(0).getDouble(0)).toInt
    }

    test("Agstr mean and total should be expected") {
      val dstr = Stratified(strdat, col("acres92"), col("region"))
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

    // test("Agstr props should be expected") {
    //   val ltstr = Stratified(lt200k, strata, stweights, stfpc)
    //   val t1 = ltstr.estimate("svymean")
    //   val t2 = ltstr.confint("svymean")
    //   assertEquals(t1(0), 0.5139)
    //   assertEquals(t1(1), 0.0248)
    //   assertEquals((t2(0) * 1000).round.toDouble/1000, 0.465)
    //   assertEquals((t2(1) * 1000).round.toDouble/1000, 0.563)
    // }


}

