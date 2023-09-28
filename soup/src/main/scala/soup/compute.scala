package conviva.soup 

object Stats {

  import org.apache.commons.math3.distribution.TDistribution
  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._

  trait  Compute {
    val dat: DataFrame
    val popSize: Column
    val weights: Column
    val strata: Column
    val alpha: Double
    //
    def tstat(alpha: Double) = udf((col: Double) => {
      new TDistribution(col).inverseCumulativeProbability(1 - (alpha / 2))
    })
    val strata_ = Option(strata).getOrElse(lit(1))
    val weight_ = Option(weights).getOrElse(col("popSize") / col("smpSize"))
    val fpc = lit(1) - (lit(1) / weight_)
    //
    def summary(y: Column): DataFrame = {
      dat
        .withColumn("strata", strata_)
        .groupBy("strata").agg(
          mean(y).alias("ybar"),
          sum(y).cast("double").alias("ysum"),
          variance(y).alias("yvar"),
          count(lit(1)).cast("double").alias("smpSize"),
          first(popSize).alias("popSize")
        )
      .withColumn("fpc", fpc)
      .withColumn("weight", weight_)
    }
    //
    def calcDf(): Column = col("smpSize") - lit(1)
    def smpMean(): Column = ((col("ysum") * col("weight")) / col("popSize")).alias("yest")
    def smpMVar(): Column = (col("fpc") * (col("yvar") / col("smpSize"))).alias("yvarFpc")
    def smpTotal(): Column = (col("ysum") * col("weight")).alias("yest")
    def smpTVar(): Column = 
        (col("fpc") * pow(col("popSize"), 2) * (col("yvar") / col("smpSize"))).alias("yvarFpc")

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

}
