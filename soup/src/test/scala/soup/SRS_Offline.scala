package conviva.soup

class SRSDesignSuite extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.soup.Design._

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    val path = "./soup/src/test/data"

    val agsrs = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agsrs2.csv")
      .withColumn("lt200k", when(col("acres92") < 2e5, 1).otherwise(0))
      .withColumn("N", lit(3078.0))

    val regionCode: Column = typedLit(Map("NE" -> 220.0, "NC" -> 1054.0, "S" -> 1382.0, "W" -> 422.0))
    val agstrat = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agstrat.csv")
      .withColumn("N", regionCode(col("region"))) 

    test("Agsrs props should be expected") {
      val srs = SRS(agsrs, popSize = col("N"))
      val t1 = srs.svymean(y = col("lt200k"))
      assertEquals(t1.select("yest").first.getDouble(0), 0.51)
      assertEquals(t1.select(round(col("yse"), 4)).first.getDouble(0), 0.0275)
      assertEquals(t1.select(round(col("lb"), 3)).first.getDouble(0), 0.456)
      assertEquals(t1.select(round(col("ub"), 3)).first.getDouble(0), 0.564)
      // val agsrs1 = agsrs.withColumn("weights", lit(3078.0/300))
      val srs1 = SRS(agsrs, weights = lit(3078.0/300))
      val t2 = srs1.svymean(y = col("lt200k"))
      assertEquals(t2.select(round(col("yest"), 3)).first.getDouble(0), 0.51)
      assertEquals(t2.select(round(col("yse"), 4)).first.getDouble(0), 0.0275)
      assertEquals(t2.select(round(col("lb"), 3)).first.getDouble(0), 0.456)
    }

    test("Agsrs means and totals should be expected") {
      val dsrs = SRS(agsrs, popSize = col("N"))
      val srs = dsrs.summary(col("acres92"))
      val t1 = dsrs.svymean(col("acres92"))
      val t2 = dsrs.svytotal(col("acres92"))
      assertEquals(srs.select(round(col("ybar"))).first.getDouble(0), 297897.0)
      assertEquals(srs.select(round(col("weight"), 3)).first.getDouble(0), 10.26)
      assertEquals(srs.select(round(col("ysum"))).first.getDouble(0), 89369114.0)
      assertEquals(t1.select(round(col("yest"))).first.getDouble(0), 297897.0)
      assertEquals(t1.select(round(col("yse"))).first.getDouble(0), 18898.0)
      assertEquals(t1.select(round(col("lb"), 1)).first.getDouble(0), 260706.3)
      assertEquals(t1.select(round(col("ub"), 1)).first.getDouble(0), 335087.8)
      assertEquals(t2.select(round(col("yest"))).first.getDouble(0), 916927110.0)
      assertEquals(t2.select(round(col("yse"))).first.getDouble(0), 58169381.0)
      assertEquals(t2.select(round(col("lb"))).first.getDouble(0), 802453859.0)
      assertEquals(t2.select(round(col("ub"))).first.getDouble(0), 1031400361.0)
    }

    test("Agsrs means by grouping variable should be expected") {
      val dsrs = SRS(agstrat, popSize = col("N"),  strata = col("region"))
      val srs = dsrs.summary(col("acres92"))
      val t1 = dsrs.svymean(col("acres92"))
      // val t0 = dsrs.svytotal()
      assertEquals(
        srs.where(col("region") === "NE")
        .select(round(col("ybar"), 1)).first.getDouble(0), 97629.8)
      assertEquals(
        srs.where(col("region") === "NC")
        .select(round(col("ybar"), 1)).first.getDouble(0), 300504.2)
      assertEquals(
        srs.where(col("region") === "W")
        .select(round(col("yvar"))).first.getDouble(0), 396185950266.0)
      assertEquals(
        t1.where(col("region") === "NE")
        .select(round(col("yest"), 1)).first.getDouble(0), 97629.8)
      assertEquals(
        t1.where(col("region") === "W")
        .select(round(col("yest"), 1)).first.getDouble(0), 662295.5)
      assertEquals(
        t1.where(col("region") === "NE")
        .select(round(col("yse"), 1)).first.getDouble(0), 18149.5)
      assertEquals(
        t1.where(col("region") === "S")
        .select(round(col("yse"), 1)).first.getDouble(0), 18925.4)
    }


    test("Agsrs svyratio and standard errors should be expected") {
      val srs = SRS(agsrs, popSize = col("N"))
      val t1 = srs.svyratio(col("acres92"), col("acres87"))
      assertEquals(t1.select(round(col("yest"), 6)).first.getDouble(0), 0.986565)
      assertEquals(t1.select(round(col("yse"), 6)).first.getDouble(0), 0.005750)
    }

    test("agstrat using weights should be expected") {
      val wts: Column = typedLit(
        Map("NE" -> 2.0, "NC" -> 1.0, "S" -> 9.0, "W" -> 7.0))
      val wcol: Column = wts(col("region"))
      val srs = SRS(agstrat, weights = wcol, strata = col("region"))
      val srs2 = SRS(agstrat, weights = lit(2.0))
      val srs3 = SRS(agstrat, weights = lit(2.0), strata = col("region"))
      // val srs4 = SRS(agstrat, popSize = col("N"), weights = wcol))
      val t1 = srs.summary(col("acres92"))
      val t2 = srs2.summary(col("acres92"))
      val t3 = srs3.summary(col("acres92"))
      assertEquals(t1.where(col("region") === "NE").select("weight").first.getDouble(0), 2.0)
      assertEquals(t1.where(col("region") === "W").select("weight").first.getDouble(0), 7.0)
      assertEquals(t2.select("weight").first.getDouble(0), 2.0)
      assertEquals(t3.where(col("region") === "W").select("weight").first.getDouble(0), 2.0)
      assertEquals(t3.where(col("region") === "NE").select("weight").first.getDouble(0), 2.0)
    }

}
