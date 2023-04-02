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
    val vprod: Column

    /** Calculate the finite population correction factor: 1 - (n/N) */
    def fpc_(n: Column, N: Column): Column = lit(1) - (n / N)
    /** Calculat the survey weight: N / n. */
    def weight_(n: Column, N: Column): Column = N / n
    /** Calculate the sampling mean and variance. `yprod` is a product or
     *  multiplier to calculate the sample mean or total, which is
     *  n/N or N, respectively. `vprod` is  the sample mean or total variance 
     *  multiplier, which is (n/N)^2 or N^2, respectively. */
    def getMean(data: DataFrame, N: Double, yprod: Column, vprod: Column): 
        DataFrame = {
      data.select(
          (col("ybar") * yprod).alias("yest"),
          (col("fpc") * vprod * (col("yvar") / col("n"))).alias("yvar"))
        .agg(
          sum("yest").alias("yest"), 
          sum("yvar").alias("yvar")
        )
        .withColumn("yse", sqrt(col("yvar")))
    }
    /** Calculate the confidence intervals. 
     *  @param alpha The default value is 0.05 for 95% conidence intervals. 
    */
    case class confint_(data: DataFrame, df: Double, alpha: Double) {
      val tstat = new TDistribution(df)
        .inverseCumulativeProbability(1 - (alpha/2))
      val width = lit(tstat) * col("yse")
      def lb(): Column = col("yest") - width 
      def ub(): Column = col("yest") + width
    }
    /** Calculate the coefficient of variation. */
    def cv_(yest: Column, yse: Column): Column = yse / yest
  }

  trait Compute extends Survey {
    
    /** Dataframe of summary statistics. For SRS, strata = 1. */
    val sdat = data.groupBy(strata)
      .agg(
        mean(y).alias("ybar"),
        variance(y).alias("yvar"),
        count("*").cast("double").alias("n"),
        first("N_").alias("N_")
      )
      .withColumn("fpc", fpc_(col("n"), col("N_")))

    /** Return the summary statistics. */
    def summary(): DataFrame = sdat 

    /** Return the population size. */
    def N(): Double = {
      sdat.agg(sum("N_")).collect()(0).getDouble(0)
    }

    /** Return the sample size. */
    def n(): Double = {
      sdat.agg(sum("n")).collect()(0).getDouble(0)
    }

    /** Return the number of strata. */
    def nstrata(): Long =  sdat.count

    /** Calculate the survey mean, with standerd error and
     *  confidence intervals.
     *  @param alpha The default value is 0.05 for 95% conidence intervals. 
     *  */
    def svymean(alpha: Double = 0.05): DataFrame = {
      val mdat = getMean(sdat, N(), 
        yprod =  (col("N_")  / N), vprod) 
      val ci = confint_(mdat, df, alpha)
      mdat.withColumn("lb", ci.lb())
        .withColumn("ub", ci.ub())
    }

    /** Calculate the survey total, with standerd error and
     *  confidence intervals.
     *  @param alpha The default value is 0.05 for 95% conidence intervals. 
     *  */
    def svytotal(alpha: Double = 0.05): DataFrame = {
      val tdat = getMean(sdat, N(), 
        yprod = col("N_"), vprod = pow(col("N_"), 2))
      val ci = confint_(tdat, df, alpha)
      tdat
        .withColumn("lb", ci.lb())
        .withColumn("ub", ci.ub())
    }
    /** Calculate the confidence intervals. */
    def cv(): DataFrame = {
      svymean().select(col("yest") / col("yse").alias("cv"))
    }
  }

}


