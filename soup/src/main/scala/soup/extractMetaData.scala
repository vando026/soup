package conviva.soup 

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.hadoop.fs._
import conviva.surgeon.Paths._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{regexp_extract, col}

object MetaData {

  case class SamplingFrame(path: String) {

   private val ss = SparkSession.builder
      .getOrCreate.sparkContext.hadoopConfiguration

    private val dbfs = FileSystem.get(ss)
    val paths = dbfs.listStatus(new Path(path))
      .map(_.getPath.toString)
      .sorted.drop(1) // drop1 drops cust=0 after sort

    def extractMeta(ipath: String): Row = {
      val getSize = dbfs.getContentSummary(new Path(ipath)).getLength)
      val size = Array("size=" + getSize)
      val info = ipath.split("/")
        .filter(_.matches("y=\\d{4}|m=\\d{2}|d=\\d{2}|dt=.*|cust=.*"))
      Row(List(info, size).flatten: _*)
    }

    def table(paths: Array[String]): DataFrame = {
      val spark = SparkSession
          .builder()
          .master("local[*]")
          .getOrCreate()

      val schema = StructType(Array(
        StructField("year", StringType, true),
        StructField("month", StringType,true),
        StructField("day",  StringType, true),
        StructField("hour", StringType, true),
        StructField("cust", StringType, true),
        StructField("size", StringType, true)
      ))

      val pathsMeta = paths.map(extractMeta(_)).toSeq
      val rdd = spark.sparkContext.parallelize(pathsMeta)
      val dat = spark.createDataFrame(rdd, schema)

      val dt =  "^dt=\\d{4}_\\d{2}_\\d{2}_(\\d{2})$"
      val st = "^\\w+=(\\d+)$"
      val cnames = List("year", "month", "day", "hour", "cust", "size")

      val ncol = pathsMeta(0).length
      val query = (0 until ncol).map(i => dat("c")(i).alias(s"${cnames(i)}"))
      val df = dat.select(query: _*)
        .withColumn("hour2", regexp_extract(col("hour"), dt, 1).cast("integer"))
        .withColumn("size", regexp_extract(col("size"), st, 1).cast("long")
        )
      df
    }
  }
}
