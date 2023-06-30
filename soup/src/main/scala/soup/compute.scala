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
  case class Summarize(
      dat: DataFrame, y: String, N: String = "N", strata: String = "") {
    def compute(): DataFrame = {
      assert(dat.columns.contains("N"), "Column called N required for population total.")
      val byVar = strata match {
        case x if x == "" => lit(1)
        case _ => col(strata)
      }
      dat.groupBy(byVar).agg(
        mean(col(y)).alias("ybar"),
        variance(col(y)).alias("yvar"),
        count(lit(1)).cast("double").alias("smpN"),
        first(col(N)).alias("N")
      )
      .withColumn("fpc", lit(1) - (col("smpN") / col("N")))
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
    def smpMean(): Column = col("ybar").alias("yest")
    def smpMVar(): Column = {
      (col("fpc") * (col("yvar") / col("smpN"))).alias("yvarFpc")
    }
    def smpTotal(): Column = (col("ybar") * col("N")).alias("yest")
    def smpTVar(): Column = 
      (col("fpc") * pow(col("N"), 2) * (col("yvar") / col("smpN"))).alias("yvarFpc")

    def getEst(data: DataFrame, alpha: Double): DataFrame = {
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

}
