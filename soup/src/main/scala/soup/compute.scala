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
     *  */ 
    def getEst(data: DataFrame, df: Double, alpha: Double): Map[String, Double] = {
      val tstat = new TDistribution(df)
        .inverseCumulativeProbability(1 - (alpha/2))
      lazy val yest = data.select(sum("yest")).first.getDouble(0)
      lazy val yvar = data.select(sum("yvar")).first.getDouble(0)
      lazy val yse = math.sqrt(yvar)
      lazy val width = tstat * yse
      lazy val lb = yest - width
      lazy val ub = yest + width
      Map("yest" -> yest, "yvar" -> yvar, "yse" -> yse, "lb" -> lb, "ub" -> ub)
    }

    def summary(): DataFrame = data

    /** Calculate the coefficient of variation. */
    // def cv_(yest: Column, yse: Column): Column = yse / yest
  
    /** Return the population size. */
    def N(): Double = 
      data.select(sum("N_")).first().getDouble(0)

    /** Return the sample size. */
    def n(): Double =
      data.select(sum("n")).first().getDouble(0)

    /** Return the number of strata. */
    def nstrata(): Long =  data.count

  }

  trait SVYMean extends Survey {
    def smpMean(N: Double): Column = (col("ybar") * col("N_") / N).alias("yest")
    def smpMVariance: Column
    def __mdat = data.select(smpMean(N()), smpMVariance)
    def yest = data.select(smpMean(N())).first.getDouble(0)
    def yvar = data.select(smpMVariance).first.getDouble(0)
    /** Calculate the survey mean, with standerd error and
     *  confidence intervals.
     *  @param alpha The default value is 0.05 for 95% conidence intervals. 
     *  */
    def svymean(alpha: Double = 0.05): Map[String, Double] = getEst(__mdat, df, alpha)
  }

  trait SVYTotal extends Survey {
    def smpTotal(): Column = (col("ybar") * col("N_")).alias("yest")
    def smpTVariance(): Column = 
      (col("fpc") * pow(col("N_"), 2) * (col("yvar") / col("n"))).alias("yvar")
    def __tdat = data.select(smpTotal, smpTVariance)
    def test = data.select(smpTotal).first.getDouble(0)
    def tvar = data.select(smpTVariance).first.getDouble(0)
    /** Calculate the survey mean, with standerd error and
     *  confidence intervals.
     *  @param alpha The default value is 0.05 for 95% conidence intervals. 
     *  */
    def svytotal(alpha: Double = 0.05): Map[String, Double] =  getEst(__tdat, df, alpha)
  }

}


