package conviva.soup

// import java.io._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.hadoop.fs._
import conviva.surgeon.Paths._
import conviva.soup.Sampler._
import org.apache.spark.sql.{SparkSession, DataFrame}

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
  val sframe = SampleFrame(path)
  

}
