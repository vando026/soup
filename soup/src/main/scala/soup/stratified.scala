package conviva.soup

import conviva.surgeon.Paths._
import conviva.soup.sampler._
import org.apache.spark.sql.{DataFrame, Column, Row}
import conviva.surgeon.PbSS._
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.sql.functions.{
  col, pow, sqrt, lit, sum, count, variance, first, mean
}

object statified {

    // override val df: Double = n() - nstrata() 
// pow(col("N_")/N(), 2))
  trait StratDesign {
    def svymean_(data: DataFrame, N: Double, prod: Column): DataFrame = {
      val sdat =  data
        .select(
          col("nh"),
          ((col("ybar") * col("Nh_")) / lit(N))
            .alias("ybar"),
          (col("fpc") * prod * (col("yvar") / col("nh")))
            .alias("yvar")
        )
        .agg(
          sum("ybar").alias("ybar"), 
          sum("yvar").alias("yvar")
        )
        .withColumn("yse", sqrt(col("yvar")))
      sdat
    }

    def confint_(data: DataFrame, df: Double, alpha: Double = 0.05): 
        DataFrame = {
      val tstat = new TDistribution(df)
        .inverseCumulativeProbability(1 - (alpha/2))
      data.select(
        (col("ybar") - lit(tstat) * col("yse")).alias("lb"),
        (col("ybar") + lit(tstat) * col("yse")).alias("ub"))
    }
  }

  case class Stratified2(data: DataFrame, y: Column, strata: Column) 
      extends StratDesign {

    val sdat = data.groupBy(strata)
      .agg(
        mean(y).alias("ybar"),
        sum(y).alias("ytot"),
        variance(y).alias("yvar"),
        count("*").cast("double").alias("nh"),
        first("strsize").alias("Nh_")
      )
      .withColumn("fpc", lit(1) - (col("nh") / col("Nh_")))

    def summary(): DataFrame = {
      sdat
    }

    def N(): Double = {
      sdat.agg(sum("Nh_")).collect()(0).getDouble(0)
    }

    def n(): Double = {
      sdat.agg(sum("nh")).collect()(0).getDouble(0)
    }

    def nstrata(): Long =  sdat.count

    val ss = svymean_(sdat, N(), pow(col("Nh_")/N(), 2))
    val ci = confint_(ss, df = n() - nstrata())
  }
}
