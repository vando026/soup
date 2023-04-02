package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.sql.{Encoder, Encoders}

object compute {

  trait Survey {
    val data: DataFrame
    val y: Column
    val strata: Column
    val df: Double
    val prod: Column

    def fpc_(n: Column, N: Column): Column = lit(1) - (n / N)
    def weight_(n: Column, N: Column): Column = N / n
    def getMean(data: DataFrame, N: Double, prod: Column): DataFrame = {
      data.select(
          ((col("ybar") * col("N_")) / N).alias("yest"),
          (col("fpc") * prod * (col("yvar") / col("n"))).alias("yvar"))
    }
    def getTotal(data: DataFrame): DataFrame = {
      data.select(
          (col("ybar") * col("N_")).alias("yest"),
          (col("fpc") * pow(col("N_"), 2) * (col("yvar") / col("n")))
            .alias("yvar"))
    }
    def collapse(data: DataFrame): DataFrame = {
        data.agg(
          sum("yest").alias("yest"), 
          sum("yvar").alias("yvar")
        )
        .withColumn("yse", sqrt(col("yvar")))
    }
    case class confint_(data: DataFrame, df: Double, alpha: Double) {
      val tstat = new TDistribution(df)
        .inverseCumulativeProbability(1 - (alpha/2))
      def lb(): Column = {
        (col("yest") - lit(tstat) * col("yse")).alias("lb")
      }
      def ub(): Column = {
        (col("yest") + lit(tstat) * col("yse")).alias("ub")
      }
    }
    def cv_(yest: Column, yse: Column): Column = yse / yest
  }

  trait Compute extends Survey {
    
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

    def svymean(alpha: Double = 0.05): DataFrame = {
      val mdat = getMean(sdat, N(), prod) 
      val cdat = collapse(mdat)
      val ci = confint_(cdat, df, alpha)
      cdat.withColumn("lb", ci.lb())
        .withColumn("ub", ci.ub())
    }

    def svytotal(alpha: Double = 0.05): DataFrame = {
      val tdat = getTotal(sdat)
      val cdat = collapse(tdat)
      val ci = confint_(tdat, df, alpha)
      cdat
        .withColumn("lb", ci.lb())
        .withColumn("ub", ci.ub())
    }
    
    def cv(): DataFrame = {
      svymean().select(col("yest") / col("yse").alias("cv"))
    }
  }

}


