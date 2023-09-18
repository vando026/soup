package conviva.soup

class SRSDesignSuite2 extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.soup.Design2._

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    val path = "./src/test/data"

    val agsrs = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agsrs2.csv")
      .withColumn("lt200k", when(col("acres92") < 2e5, 1).otherwise(0))
      .withColumn("N", lit(3078.0))

    val agstrat = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agstrat.csv")
      .withColumn("N", 
        when(col("region") === "NE", 220.0)
        .when(col("region") === "NC", 1054.0)
        .when(col("region") === "S", 1382.0)
        .when(col("region") === "W", 422.0))

    test("Agsrs props should be expected") {
      val srs = Design(agsrs, popSize = col("N"))
      val t1 = svymean(y = col("lt200k"), srs)
      assertEquals(t1.select("yest").first.getDouble(0), 0.51)
      assertEquals(t1.select(round(col("yse"), 4)).first.getDouble(0), 0.0275)
      assertEquals(t1.select(round(col("lb"), 3)).first.getDouble(0), 0.456)
      assertEquals(t1.select(round(col("ub"), 3)).first.getDouble(0), 0.564)
    }

    test("Agsrs means and totals should be expected") {
      val dsrs = Design(agsrs, popSize = col("N"))
      val srs = dsrs.summary(col("acres92"))
      val t1 = svymean(col("acres92"), dsrs)
      val t2 = svytotal(col("acres92"), dsrs)
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
      val dsrs = Design(agstrat, popSize = col("N"),  strata = col("region"))
      val srs = dsrs.summary(col("acres92"))
      val t1 = svymean(col("acres92"), dsrs)
      // val t0 = dsrs.svytotal()
      assertEquals(
        srs.where(col("strata") === "NE")
        .select(round(col("ybar"), 1)).first.getDouble(0), 97629.8)
      assertEquals(
        srs.where(col("strata") === "NC")
        .select(round(col("ybar"), 1)).first.getDouble(0), 300504.2)
      assertEquals(
        srs.where(col("strata") === "W")
        .select(round(col("yvar"))).first.getDouble(0), 396185950266.0)
      assertEquals(
        t1.where(col("strata") === "NE")
        .select(round(col("yest"), 1)).first.getDouble(0), 97629.8)
      assertEquals(
        t1.where(col("strata") === "W")
        .select(round(col("yest"), 1)).first.getDouble(0), 662295.5)
      assertEquals(
        t1.where(col("strata") === "NE")
        .select(round(col("yse"), 1)).first.getDouble(0), 18149.5)
      assertEquals(
        t1.where(col("strata") === "S")
        .select(round(col("yse"), 1)).first.getDouble(0), 18925.4)
    }


    test("Agsrs svyratio and standard errors should be expected") {
      val srs = Design(agsrs, popSize = col("N"))
      val t1 = svyratio(col("acres92"), col("acres87"), srs)
      assertEquals(t1.select(round(col("yest"), 6)).first.getDouble(0), 0.986565)
      assertEquals(t1.select(round(col("yse"), 6)).first.getDouble(0), 0.005750)
    }

}
