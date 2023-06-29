package conviva.soup

class SRSDesignSuite extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.soup.Design.{Simple}
  import conviva.soup.Compute._

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    val path = "./src/test/data"

    val srs_ = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agsrs.csv")
      .select(
        col("acres92"),
        when(col("acres92") < 2e5, 1).otherwise(0).alias("lt200k")
      )
      .withColumn("N", lit(3078.0))

    val strdat_ = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agstrat.csv")
      .withColumn("N", 
        when(col("region") === "NE", 220.0)
        .when(col("region") === "NC", 1054.0)
        .when(col("region") === "S", 1382.0)
        .when(col("region") === "W", 422.0))

    test("Agsrs props should be expected") {
      val srs = Summarize(srs_, y = "lt200k").compute
      val ltsrs = Simple(srs)
      val t1 = ltsrs.svymean()
      assertEquals(t1.select("yest").first.getDouble(0), 0.51)
      assertEquals(t1.select(round(col("yse"), 4)).first.getDouble(0), 0.0275)
      assertEquals(t1.select(round(col("lb"), 3)).first.getDouble(0), 0.456)
      assertEquals(t1.select(round(col("ub"), 3)).first.getDouble(0), 0.564)
    }

    test("Agsrs means and totals should be expected") {
      val srs = Summarize(srs_, y = "acres92").compute
      val dsrs = Simple(srs)
      val t1 = dsrs.svymean()
      val t2 = dsrs.svytotal()
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
      val srs = Summarize(strdat_, y = "acres92", strata = "region").compute
      val dsrs = Simple(srs)
      val t1 = dsrs.svymean()
      // val t0 = dsrs.svytotal()
      assertEquals(
        srs.where(col("region") === "NE")
        .select(round(col("ybar"), 1)).first.getDouble(0), 97629.8
      )
      assertEquals(
        srs.where(col("region") === "W")
        .select(round(col("yvar"))).first.getDouble(0), 396185950266.0
      )
      assertEquals(
        t1.where(col("region") === "NE")
        .select(round(col("yest"), 1)).first.getDouble(0), 97629.8
      )
      assertEquals(
        t1.where(col("region") === "W")
        .select(round(col("yest"), 1)).first.getDouble(0), 662295.5
      )
      assertEquals(
        t1.where(col("region") === "NE")
        .select(round(col("yse"), 1)).first.getDouble(0), 18149.5
      )
      assertEquals(
        t1.where(col("region") === "S")
        .select(round(col("yse"), 1)).first.getDouble(0), 18925.4
      )
    }

}
