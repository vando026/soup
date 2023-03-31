package conviva.soup

import java.io._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.hadoop.fs._
import conviva.surgeon.Paths._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{regexp_replace}

class PathSpec extends munit.FunSuite {

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  // val dpath = "./src/test/data/"
  // val ois = new ObjectInputStream(new java.io.FileInputStream(s"$dpath/pathsHourly2_24"))
  // val pathx = ois.readObject.asInstanceOf[Array[String]]
  // ois.close

  val path = Hourly(2, 23, List(2)).toPath
  val ss = SparkSession.builder
    .getOrCreate.sparkContext.hadoopConfiguration
  val dbfs = FileSystem.get(ss)
  val paths = dbfs.listStatus(new Path(path))
    .map(_.getPath.toString)
    .sorted.drop(1) // drop1 drops cust=0 after sort

  def extract(ipath: String): Row = {
    val size = Array("size=" + dbfs.getContentSummary(new Path(ipath)).getLength)
    val info = ipath.split("/")
      .filter(_.matches("y=\\d{4}|m=\\d{2}|d=\\d{2}|dt=.*|cust=.*"))
    Row(List(info, size).flatten: _*)
  }

  val schema = StructType(Array(
    StructField("year", StringType, true),
    StructField("month", StringType,true),
    StructField("day",  StringType, true),
    StructField("hour", StringType, true),
    StructField("cust", StringType, true),
    StructField("size", StringType, true)
  ))

  val rows = paths.slice(0, 5).map(extract(_)).toSeq
  val rdd = spark.sparkContext.parallelize(rows)
  val dat = spark.createDataFrame(rdd, schema)

  val ncol = rows(0).length
  val dt =  "^dt=\\d{4}_\\d{2}_\\d{2}_(\\d{2})$"
  val st = "^\\w+=(\\d+)$"
  val cnames = List("year", "month", "day", "hour", "cust", "size")
  val tr = (0 until ncol).map(i => dat("c")(i).alias(s"${cnames(i)}"))
  val df = dat.select(tr: _*)
  val df1 = df
    .withColumn("hour2", regexp_extract(col("hour"), dt, 1).cast("integer"))
    .withColumn("size", regexp_extract(col("size"), st, 1).cast("long")
    )
  df1.show


}
