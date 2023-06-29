package conviva.soup

class StratDesignSuit extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.soup.Design.{Stratified}
  import conviva.soup.Compute._

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


    test("Agstr mean and total should be expected") {
      val strdat = Summarize(strdat_, y = col("acres92"), strata = col("region")).compute
      val dstr = Stratified(strdat)
      val t0 = dstr.svymean()
      val t1 = dstr.svytotal()
      // assertEquals(t0("yest").ceil, 295561.0)
      // assertEquals(t0("yse").ceil, 16380.0)
      // assertEquals(t0("lb").ceil, 263325.0)
      // assertEquals(t0("ub").ceil, 327797.0)
      // assertEquals(t1("yest").toInt, 909736035)
      // assertEquals(t1("yse").toInt, 50417248)
      // assertEquals(t1("lb").toInt, 810514349)
      // assertEquals(t1("ub").toInt, 1008957720)
    }

    test("Agstr props should be expected") {
      val strdat = Summarize(strdat_, y = col("lt200k"), strata = col("region")).compute
      val ltstr = Stratified(strdat)
      val t1 = ltstr.svymean()
      // assertEquals((t1("yest") * 10000).round.toDouble/10000, 0.5139)
      // assertEquals((t1("yse") * 10000).round.toDouble/10000, 0.0248)
      // assertEquals((t1("lb") * 1000).round.toDouble/1000, 0.465)
      // assertEquals((t1("ub") * 1000).round.toDouble/1000, 0.563)
    }

}

