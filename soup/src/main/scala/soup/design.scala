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
        ((ybar * Nh) / lit(N)).alias("ybarw")
    }
    def wvar_(yvar: Column, fpc: Column, n: Column, prod: Column): Column = {
          (fpc * prod * (yvar / n))
    }
    val prod: Column
    def getEstimates(data: DataFrame, N: Double, prod: Column): DataFrame = {
      val sdat =  data
        .select(
          wmean_(col("ybar"), col("N_"), N).alias("ybar"),
          wvar_(col("yvar"), col("fpc"), col("n"), prod).alias("yvar")
        )
        .agg(
          sum("ybar").alias("ybar"), 
          sum("yvar").alias("yvar")
        )
        .withColumn("yse", sqrt(col("yvar")))
      sdat
    }
    val df: Double
    def confint_(data: DataFrame, df: Double, alpha: Double): 
        DataFrame = {
      val tstat = new TDistribution(df)
        .inverseCumulativeProbability(1 - (alpha/2))
      data.select(
        (col("ybar") - lit(tstat) * col("yse")).alias("lb"),
        (col("ybar") + lit(tstat) * col("yse")).alias("ub"))
    }

  }

  case class Compute(data: DataFrame, y: Column, strata: Column = lit(1)) 
    extends Survey {
    
    val sdat = data.groupBy(strata)
      .agg(
        mean(y).alias("ybar"),
        variance(y).alias("yvar"),
        count("*").cast("double").alias("n"),
        first("N_").alias("N_")
      )
      .withColumn("fpc", fpc_(col("n"), col("N_")))

    def summary(): DataFrame = sdat 

    def N(): Double = {
      sdat.agg(sum("N_")).collect()(0).getDouble(0)
    }

    def n(): Double = {
      sdat.agg(sum("n")).collect()(0).getDouble(0)
    }

    def nstrata(): Long =  sdat.count

    override val prod: Column = lit(1)
    def svymean(): DataFrame = {
      getEstimates(sdat, N(), prod) 
    }
    

    override val df: Double = n() - 1
    def confint(alpha: Double = 0.05): DataFrame = {
      val sdat = svymean()
      confint_(sdat, df, alpha)
    }
  }
}


