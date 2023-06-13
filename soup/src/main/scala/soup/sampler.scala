package conviva.soup 

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.hadoop.fs._
import conviva.surgeon.Paths._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{regexp_extract, col}
import scala.collection.JavaConversions._

object Sampler {

  /** Construct a sample frame from a path to the Conviva datasets on Databricks. 
   *  @param dbPath A Databricks filepath  path.
   */ 
  case class SampleFrame(dbPath: String) {

    private val sparkSession = SparkSession
        .builder()
        .master("local[*]")
        .getOrCreate()

     import sparkSession.implicits._
     private val ss = SparkSession.builder
        .getOrCreate.sparkContext.hadoopConfiguration

      private val dbfs = FileSystem.get(ss)

      /** Returns a list of Databricks filepaths.*/
      val paths = dbfs.listStatus(new Path(dbPath))
        .map(_.getPath.toString)
        .sorted.drop(1) // drop1 drops cust=0 after sort

      /** Extract the metadata from the paths list. */
      private def extractMeta(ipath: String): List[String] = {
        val getSize = dbfs.getContentSummary(new Path(ipath)).getLength
        val size = Array("size=" + getSize)
        val info = ipath.split("/")
          .filter(_.matches("y=\\d{4}|m=\\d{2}|d=\\d{2}|dt=.*|cust=.*"))
        List(info, size).flatten
      }

      /** Construct a DataFrame from the metadata to enable sampling.*/
      def table(): DataFrame = {

        val pathsMeta = paths.map(extractMeta(_)).toSeq
        val dat = pathsMeta.toDF("c")

        val ncol = pathsMeta(0).length
        val cnames = List("year", "month", "day", "hour", "cust", "size")
        val query = (0 until ncol).map(i => dat("c")(i).alias(s"${cnames(i)}"))

        val dt =  "^dt=\\d{4}_\\d{2}_\\d{2}_(\\d{2})$"
        val st = "^\\w+=(\\d+)$"

        val df = dat.select(query: _*)
          .withColumn("hour2", regexp_extract(col("hour"), dt, 1).cast("integer"))
          .withColumn("size", regexp_extract(col("size"), st, 1).cast("long")
          )
        df
      }
  }
}
