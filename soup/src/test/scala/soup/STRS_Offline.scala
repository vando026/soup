package conviva.soup

class StratDesignSuit extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.soup.Design.STRS

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
      .withColumn("N", 
        when(col("region") === "NE", 220.0)
        .when(col("region") === "NC", 1054.0)
        .when(col("region") === "S", 1382.0)
        .when(col("region") === "W", 422.0))


    test("Agstr mean and total should be expected") {
      val dstr = STRS(strdat_, popSize = col("N"), strata = col("region"))
      val t0 = dstr.svymean(col("acres92"))
      val t1 = dstr.svytotal(col("acres92"))
      assertEquals(t0.select(round(col("yest"))).first.getDouble(0), 295561.0)
      assertEquals(t0.select(round(col("yse"))).first.getDouble(0), 16380.0)
      assertEquals(t0.select(round(col("lb"))).first.getDouble(0), 263325.0)
      assertEquals(t0.select(round(col("ub"), 1)).first.getDouble(0), 327796.5)
      assertEquals(t1.select(round(col("yest"))).first.getDouble(0), 909736035.0)
      assertEquals(t1.select(round(col("yse"))).first.getDouble(0), 50417248.0)
      assertEquals(t1.select(round(col("lb"))).first.getDouble(0), 810514350.0)
      assertEquals(t1.select(round(col("ub"))).first.getDouble(0), 1008957721.0)
    }

    test("Agstr props should be expected") {
      val ltstr = STRS(strdat_, popSize = col("N"), strata =  col("region") )
      val t1 = ltstr.svymean(col("lt200k"))
      assertEquals(t1.select(round(col("yest"), 4)).first.getDouble(0), 0.5139)
      assertEquals(t1.select(round(col("yse"),4)).first.getDouble(0), 0.0248)
      assertEquals(t1.select(round(col("lb"), 4)).first.getDouble(0), 0.4651)
      assertEquals(t1.select(round(col("ub"), 4)).first.getDouble(0), 0.5627)
    }

}

