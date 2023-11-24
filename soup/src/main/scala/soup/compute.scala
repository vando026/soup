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
    if (Option(weights) == None && Option(popSize) == None) 
      throw new RuntimeException("Specify either weights OR popSize")
    if (Option(weights) == Some(weights) & Option(popSize) == Some(popSize)) 
      throw new RuntimeException("Cannot specify both weights AND popSize")
    val strata_ = Option(strata).getOrElse(lit(1))
    val popSizeCompute: Column = Option(popSize) match {
      case Some(popSize) => first(popSize).alias("popSize")
      case None => sum(weights).alias("popSize")
    }

    def tstat(alpha: Double) = udf((col: Double) => {
      new TDistribution(col).inverseCumulativeProbability(1 - (alpha / 2))
    })
    //
    def summary(y: Column): DataFrame = {
      dat.groupBy(strata_)
        .agg(
          mean(y).alias("ybar"),
          sum(y).cast("double").alias("ysum"),
          variance(y).alias("yvar"),
          count(lit(1)).alias("smpSize"),
          popSizeCompute
        )
        .withColumn("fpc", lit(1) - (col("smpSize") / col("popSize")))
        .withColumn("weight", col("popSize") / col("smpSize"))
        .drop("1")
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
