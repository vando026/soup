package conviva.soup 

object Design2 {

  import org.apache.commons.math3.distribution.TDistribution
  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._

  trait Design {
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
    val fpc = lit(1) - (col("smpSize") / col("popSize"))
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
        .select(data.columns(0), "yest", "yse", "lb", "ub", "df", "tstat", "smpSize", "popSize")
    }
  }

  case class SRS(dat: DataFrame, popSize: Column, weights: Column = null, 
      strata: Column = null, alpha: Double = 0.05 ) extends Design { 
    def svymean(y: Column): DataFrame = { 
      val dat = summary(y)
      val meanData = dat.select(col("*"), smpMean, smpMVar)
      getEst(meanData)
    }
    def svytotal(y: Column): DataFrame = {
      val dat = summary(y)
      val totData = dat.select(col("*"), smpTotal, smpTVar)
      getEst(totData)
    }
    def svyratio(num: Column, den: Column): DataFrame = { 
      val y = summary(num)
        .select("strata", "ybar", "fpc", "smpSize", "popSize")
      val x = summary(den)
        .select(col("strata"), col("ybar").alias("xbar"))
      val xydat = y.join(x, List("strata"), "inner")
        .withColumn("ratio", col("ybar") / col("xbar"))
      val sdat = dat
        .withColumn("strata", strata_)
        .join(xydat, List("strata"), "left")
        .withColumn("residuals", (num - (col("ratio") * den)))
      //
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
        getEst(ratioDat)
    }
  }


  case class STRS(dat: DataFrame, popSize: Column, weights: Column = null, 
      strata: Column = null, alpha: Double = 0.05 ) extends Design { 
    override def calcDf: Column = col("smpN") - lit(dat.count)
    val bigN = dat.select(sum(popSize)).first.getDouble(0)
    val vars = List("yest", "yvarFpc", "smpSize", "popSize")
      .map(i => sum(col(i)).alias(i))
    //
    override def smpMean(): Column = (col("ybar") * col("popSize") / lit(bigN)).alias("yest")
    override def smpMVar(): Column = {
      (col("fpc") * pow(col("N") / lit(bigN), 2) * (col("yvar") / col("smpSize"))).alias("yvarFpc")
    }
    def svymean(y: Column): DataFrame = {
      val data = summary(y)
      val meanData = data.select(smpMean, smpMVar, col("smpSize"), col("popSize"))
        .agg(vars.head, vars.tail:_*)
      getEst(meanData)
    }
    //
    def svytotal(y: Column): DataFrame =  {
      val data = summary(y)
      val totData = data.select(smpTotal, smpTVar, col("smpSize"), col("popSize"))
          .agg(vars.head, vars.tail:_*)
      getEst(totData)
    }

  }

}
