package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.soup.Design.{Simple}

class SRSDesignSuite extends munit.FunSuite {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

    val path = "./src/test/data"

    val srs = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"$path/agsrs.csv")
      .select(
        col("acres92"),
        when(col("acres92") < 2e5, 1).otherwise(0).alias("lt200k")
      )
      .withColumn("N_", lit(3078.0))

    def exd(x: String, d: DataFrame): Double = {
      d.select(col(x)).collect()(0).getDouble(0)
    }

    test("Agsrs props should be expected") {
      val ltsrs = Simple(srs, col("lt200k"))
      val t1 = ltsrs.svymean()
      assertEquals(exd("yest", t1), 0.51)
      assertEquals((exd("yse", t1) * 10000).round.toDouble/10000, 0.0275)
      assertEquals((exd("lb", t1) * 100).round.toDouble/100, 0.46)
      assertEquals((exd("ub", t1) * 100).round.toDouble/100, 0.56)
    }

    def ex(x: String, d: DataFrame): Int = {
      d.select(col(x)).collect()(0).getDouble(0).toInt
    }

    test("Agsrs means should be expected for Simple") {
      val dsrs = Simple(srs, col("acres92"))
      val t1 = dsrs.svymean()
      val t2 = dsrs.svytotal()
      assertEquals(ex("yest", t1), 297897)
      assertEquals(ex("yse", t1), 18898)
      assertEquals(ex("lb", t1), 260706)
      assertEquals(ex("ub", t1), 335087)
      assertEquals(ex("yest", t2), 916927109)
      assertEquals(ex("yse", t2), 58169381)
      assertEquals(ex("lb", t2), 802453858)
      assertEquals(ex("ub", t2), 1031400360)
    }

}

