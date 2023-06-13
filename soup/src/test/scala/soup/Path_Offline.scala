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
}
