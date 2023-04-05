package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.commons.math3.distribution.TDistribution

object Compute {

  /** Prepare the data for estimation by taking mean and variance of y with n
   *  and N. 
   *  @param data The input dataframe. 
   *  */
  case class Summarize(dat: DataFrame, y: Column, strata: Column = lit(1)) {
    def data(): DataFrame = {
      dat.groupBy(strata)
       .agg(
         mean(y).alias("ybar"),
         variance(y).alias("yvar"),
         count("*").cast("double").alias("n"),
         first("N_").alias("N_")
       )
       .withColumn("fpc", lit(1) - (col("n") / col("N_")))
    }
  }

  trait Survey {
    val data: DataFrame
    val df: Double
    def smpMVariance: Column

    /** Get the final estimates from sampled data with standard error and
     *  confidence intervals.
     *  @param data A dataset processed from `Summarized`.
     *  @param df The degrees of freedom, computed internally.
     *  @param alpha The alpha for t-distribution based confidence intervals.
     *  */ 
    def getEst(data: DataFrame, df: Double, alpha: Double): DataFrame = {
      val tstat = new TDistribution(df)
        .inverseCumulativeProbability(1 - (alpha/2))
      val adat = data.agg(
          sum("yest").alias("yest"), 
          sum("yvar").alias("yvar")
        )
        .withColumn("yse", sqrt(col("yvar")))
      val width = lit(tstat) * col("yse")
      adat
       .withColumn("lb", col("yest") - width)
       .withColumn("ub", col("yest") + width)
    }

    def summary(): DataFrame = data

    /** Calculate the coefficient of variation. */
    def cv_(yest: Column, yse: Column): Column = yse / yest
  
    /** Return the population size. */
    def N(): Double = 
      data.agg(sum("N_")).collect()(0).getDouble(0)

    /** Return the sample size. */
    def n(): Double =
      data.agg(sum("n")).collect()(0).getDouble(0)

    /** Return the number of strata. */
    def nstrata(): Long =  data.count

  }

  trait SVYMean extends Survey {
    def smpMean(N: Double): Column = (col("ybar") * col("N_") / N).alias("yest")
    def smpMVariance: Column
    val mdat = data.select(smpMean(N()), smpMVariance)
    /** Calculate the survey mean, with standerd error and
     *  confidence intervals.
     *  @param alpha The default value is 0.05 for 95% conidence intervals. 
     *  */
    def svymean(alpha: Double = 0.05): DataFrame = getEst(mdat, df, alpha)
  }

  trait SVYTotal extends Survey {
    def smpTotal(): Column = (col("ybar") * col("N_")).alias("yest")
    def smpTVariance(): Column = 
      (col("fpc") * pow(col("N_"), 2) * (col("yvar") / col("n"))).alias("yvar")
    def tdat = data.select(smpTotal, smpTVariance)
    /** Calculate the survey mean, with standerd error and
     *  confidence intervals.
     *  @param alpha The default value is 0.05 for 95% conidence intervals. 
     *  */
    def svytotal(alpha: Double = 0.05): DataFrame =  getEst(tdat, df, alpha)
  }

}


