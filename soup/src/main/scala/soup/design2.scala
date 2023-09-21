package conviva.soup 

object Design2 {

  import org.apache.commons.math3.distribution.TDistribution
  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._

  case class Design(
       dat: DataFrame,
       popSize: Column = null,
       strata: Column = null,
       weights: Column = null,
       alpha: Double = 0.05) {
    //
    val strataVar = Option(strata).getOrElse(lit(1))
    val wtVar = Option(weights).getOrElse(col("popSize") / col("smpSize"))
    val fpc = lit(1) - (col("smpSize") / col("popSize"))
    val popSizeVar = Option(popSize).getOrElse(col("smpSize") * wtVar) 
    //
    def tstat(alpha: Double) = udf((col: Double) => {
      new TDistribution(col).inverseCumulativeProbability(1 - (alpha / 2))
    })
    def summary(y: Column): DataFrame = {
      dat.withColumn("strata", strataVar)
        .groupBy("strata").agg(
          mean(y).alias("ybar"),
          sum(y).cast("double").alias("ysum"),
          variance(y).alias("yvar"),
          count(lit(1)).cast("double").alias("smpSize"),
          first(popSize).alias("popSize")
        )
      .withColumn("fpc", fpc)
      .withColumn("weight", wtVar)
    }
    //
    def calcDf(): Column = col("smpSize") - lit(1)
    def getEst(data: DataFrame): DataFrame = {
       data
        .withColumn("yse",  sqrt(col("yvarFpc")))
        .withColumn("df", calcDf)
        .withColumn("tstat", tstat(alpha)(col("df")))
        .withColumn("width", col("tstat") * col("yse"))
        .withColumn("lb", col("yest") - col("width"))
        .withColumn("ub", col("yest") + col("width"))
        .select(data.columns(0), "yest", "yse", "lb", "ub", "df", "tstat", "smpSize", "popSize")
    }
  }

  def svymean(y: Column, design: Design): DataFrame = {
    val dat0 = design.summary(y)
    def smpMean(): Column = ((col("ysum") * col("weight")) / col("popSize")).alias("yest")
    def smpMVar(): Column = (col("fpc") * (col("yvar") / col("smpSize"))).alias("yvarFpc")
    val meanData = dat0.select(col("*"), smpMean, smpMVar)
    design.getEst(meanData) 
  }

  def svytotal(y: Column, design: Design): DataFrame = {
    val dat0 = design.summary(y)
    def smpTotal(): Column = (col("ysum") * col("weight")).alias("yest")
    def smpTVar(): Column = 
        (col("fpc") * pow(col("popSize"), 2) * (col("yvar") / col("smpSize"))).alias("yvarFpc")
    val totData = dat0.select(col("*"), smpTotal, smpTVar)
    design.getEst(totData)
  }


  def svyratio(num: Column, den: Column, design: Design): DataFrame = { 
    val y = design.summary(num)
      .select("strata", "ybar", "fpc", "smpSize")
    val x = design.summary(den)
      .select(col("strata"), col("ybar").alias("xbar"))
    val xydat = y.join(x, List("strata"), "inner")
      .withColumn("ratio", col("ybar") / col("xbar"))
    val sdat = design.dat
      .withColumn("strata", design.strataVar)
      .join(xydat, List("strata"), "left")
      .withColumn("residuals", (num - (col("ratio") * den)))

    val ratioDat = sdat.groupBy("strata").agg(
        variance(col("residuals")).alias("resid"),
        first("ratio").alias("yest"),
        first("xbar").alias("xbar"),
        first("fpc").alias("fpc"),
        first("smpSize").alias("smpSize"),
        first("popSize").alias("popSize")
      )
      .withColumn("term1", col("smpSize") * col("xbar") * col("xbar"))
      .withColumn("yvarFpc", col("fpc") * (col("resid") / col("term1")) )
      .select("strata", "yest", "smpSize", "popSize", "yvarFpc")
      design.getEst(ratioDat)
  }

}
