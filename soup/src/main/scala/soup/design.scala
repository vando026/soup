package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.sql.{Encoder, Encoders}

object design {

  trait Survey {
    def fpc_(n: Column, N: Column): Column = lit(1) - (n / N)
    def weight_(n: Column, N: Column): Column = N / n
    def wmean_(ybar: Column, Nh: Column, N: Double): Column = {
        ((ybar * Nh) / lit(N)).alias("ybarw"),
    } 
  }


}


