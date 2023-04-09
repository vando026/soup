package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.soup.Design.{Simple}
import conviva.soup.Compute._

class SRSDesignSuite extends munit.FunSuite {

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
      .withColumn("N_", lit(3078.0))

    test("Agsrs props should be expected") {
      val srs = Summarize(srs_, col("lt200k")).data
      val ltsrs = Simple(srs)
      val t1 = ltsrs.svymean()
      assertEquals(t1("yest"), 0.51)
      assertEquals((t1("yse") * 10000).round.toDouble/10000, 0.0275)
      assertEquals((t1("lb") * 100).round.toDouble/100, 0.46)
      assertEquals((t1("ub") * 100).round.toDouble/100, 0.56)
    }

    test("Agsrs means should be expected for Simple") {
      val srs = Summarize(srs_, col("acres92")).data
      val dsrs = Simple(srs)
      val t1 = dsrs.svymean()
      val t2 = dsrs.svytotal()
      assertEquals(t1("yest").toInt, 297897)
      assertEquals(t1("yse").toInt, 18898)
      assertEquals(t1("lb").toInt, 260706)
      assertEquals(t1("ub").toInt, 335087)
      assertEquals(t2("yest").toInt, 916927109)
      assertEquals(t2("yse").toInt, 58169381)
      assertEquals(t2("lb").toInt, 802453858)
      assertEquals(t2("ub").toInt, 1031400360)
    }

}

