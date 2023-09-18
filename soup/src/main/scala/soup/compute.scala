package conviva.soup

/* TODO: Add N parameter to Summarize
 *  return N as value
 *  return data
 * Have different Summarize for Cluster
 * Ratio estimation
*/

object Compute {

  import org.apache.commons.math3.distribution.TDistribution
  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._

  /** Prepare the data for estimation by taking mean and variance of y with
   *  sample size smpN and population size N. 
   *  @param data The input dataframe. 
   *  @param y The column name of the quantity or variable to estimate.
   *  @param N The column name for the population size.
   *  @param strata  The name of the column to stratify or group the estimates by.
   *  */
  trait Compute {
    dat: DataFrame
    popSize: Column
    strata: Column = null
    weights: Column = null
    alpha: Double = 0.05
    //
    val strataVar = Option(strata).getOrElse(lit(1))
    val wtVar = Option(weights).getOrElse(col("popSize") / col("smpSize"))
    val fpc = lit(1) - (col("smpSize") / col("popSize"))
    //
    def tstat(alpha: Double) = udf((col: Double) => {
      new TDistribution(col).inverseCumulativeProbability(1 - (alpha / 2))
    })
    def summary(y: Column): DataFrame = {
      dat.groupBy(strataVar).agg(
        mean(y).alias("ybar"),
        sum(y).cast("double").alias("ysum"),
        variance(y).alias("yvar"),
        count(lit(1)).cast("double").alias("smpSize"),
        first(popSize).alias("popSize")
      )
      .withColumn("fpc", fpc)
      .withColumn("weight", wtVar)
    }

    def calcDf(): Column = col("smpN") - lit(1)
    def getEst(data: DataFrame): DataFrame = {
       data
        .withColumn("yse",  sqrt(col("yvarFpc")))
        .withColumn("df", calcDf)
        .withColumn("tstat", tstat(alpha)(col("df")))
        .withColumn("width", col("tstat") * col("yse"))
        .withColumn("lb", col("yest") - col("width"))
        .withColumn("ub", col("yest") + col("width"))
        .select(data.columns(0), "yest", "yse", "lb", "ub", "df", "tstat")
    }
  }

  trait Survey {

    /** Get the final estimates from sampled data with standard error and
     *  confidence intervals.
     *  @param data A dataset processed from `Summarized`.
     *  */ 
    val data: DataFrame
    def tstat(alpha: Double) = udf((col: Double) => {
      new TDistribution(col).inverseCumulativeProbability(1 - (alpha / 2))
    })
    def calcDf(): Column = col("smpN") - lit(1)

    def smpMean(): Column = ((col("ysum") * col("weight")) / col("N")).alias("yest")
    def smpMVar(): Column = {
      (col("fpc") * (col("yvar") / col("smpN"))).alias("yvarFpc")
    }

    def smpTotal(): Column = (col("ysum") * col("weight")).alias("yest")
    def smpTVar(): Column = 
      (col("fpc") * pow(col("N"), 2) * (col("yvar") / col("smpN"))).alias("yvarFpc")

  }

}
