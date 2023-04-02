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

    // val n = srs.count.toInt
    // val N: Double = 3078
    // val acres92 = srs.select(col("acres92"))
    //   .collect.map(_.getInt(0).toDouble)
    // val lt200k = srs.select(col("lt200k"))
    //   .collect.map(_.getInt(0).toDouble)
    // val fpc = Array.fill(n)(N)
    // val pweights = Array.fill(n)(N/n)

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


    // test("Agsrs means should be expected") {
    //   val dsrs = Simple(acres92, weights = pweights, fpc = fpc )
    //   val t0 = dsrs.estimate("svymean").map(_.toInt)
    //   val t1 = dsrs.confint("svymean").map(_.toInt)
    //   val t2 = dsrs.estimate("svytotal").map(_.toInt)
    //   val t3 = dsrs.confint("svytotal").map(_.toInt)
    //   assertEquals(t0(0), 297897)
    //   assertEquals(t0(1), 18898)
    //   assertEquals(t1(0), 260706)
    //   assertEquals(t1(1), 335087)
    //   assertEquals(t2(0), 916927109)
    //   assertEquals(t2(1), 58169381)
    //   assertEquals(t3(0), 802453858)
    //   assertEquals(t3(1), 1031400360)
    // }

    // test("Agsrs props should be expected") {
    //   val ltsrs = Simple(lt200k, weights = pweights, fpc = fpc)
    //   val t1 = ltsrs.estimate("svymean")
    //   val t2 = ltsrs.confint("svymean")
    //   assertEquals(t1(0), 0.51)
    //   assertEquals(t1(1), 0.0275)
    //   assertEquals((t2(0) * 100).round.toDouble/100, 0.46)
    //   assertEquals((t2(1) * 100).round.toDouble/100, 0.56)
    // }

    def ex(x: String, d: DataFrame): Int = {
      d.select(col(x)).collect()(0).getDouble(0).toInt
    }

    test("Agsrs means should be expected for Simple2") {
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

