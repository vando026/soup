package conviva.soup 

object Design {

  import org.apache.spark.sql.{DataFrame, Column}
  import org.apache.spark.sql.functions.{variance, pow, lit, col, sum, mean, first}
  import conviva.soup.Stats._

  case class SRS(dat: DataFrame, popSize: Column, weights: Column = null, 
      strata: Column = null, alpha: Double = 0.05 ) extends Compute { 
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
        .select(strata_, col("ybar"), col("fpc"), col("smpSize"))
      val x = summary(den)
        .select(strata_, col("ybar").alias("xbar"))
      val byVar = y.columns.intersect(x.columns).toList
      val xydat = y.join(x, byVar, "inner")
        .withColumn("ratio", col("ybar") / col("xbar"))
      val sdat = dat.select(strata_, num, den)
        .join(xydat, byVar, "left")
        .withColumn("residuals", (num - (col("ratio") * den)))
      //
      val ratioDat = sdat.groupBy(strata_).agg(
          variance(col("residuals")).alias("resid"),
          first("ratio").alias("yest"),
          first("xbar").alias("xbar"),
          first("fpc").alias("fpc"),
          first("smpSize").alias("smpSize")
        )
        .withColumn("term1", col("smpSize") * col("xbar") * col("xbar"))
        .withColumn("yvarFpc", col("fpc") * (col("resid") / col("term1")) )
        getEst(ratioDat)
    }
  }


  case class STRS(dat: DataFrame, popSize: Column, weights: Column = null, 
      strata: Column = null, alpha: Double = 0.05 ) extends Compute { 
    private val sdat = dat.groupBy("region").agg(first(popSize))
    private val bigN = sdat.agg(sum("first(N)")).first.getDouble(0)
    override def calcDf: Column = col("smpSize") - lit(sdat.count)
    private val vars = List("yest", "yvarFpc", "smpSize", "popSize")
      .map(i => sum(col(i)).alias(i))
    //
    override def smpMean(): Column = (col("ybar") * col("popSize") / lit(bigN)).alias("yest")
    override def smpMVar(): Column = {
      (col("fpc") * pow(col("popSize") / lit(bigN), 2) * (col("yvar") / col("smpSize"))).alias("yvarFpc")
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

//   case class Clust2Stage(data: DataFrame) extends Survey {
//     import org.apache.spark.sql.expressions.Window
//     val dw = Window.partitionBy(lit(1))
//     // val n = data.count
//     override def calcDf = lit(data.count - 1)
//     // calculate ytot and yvar

//     // see table 5.7 for calc in Lohr
//     val allN = data.select(sum(col("N"))).first.getLong(0).toDouble
//     val smallN = data.select(sum(col("smpN"))).first.getLong(0).toDouble
//     // col 5
//     val cdat = data
//       .withColumn("ytot", col("ybar") * col("N"))
//     val ytotSum = cdat.select(sum(col("ytot"))).first.getDouble(0)
//     val yest = ytotSum / allN
//     val cdat1 = cdat.withColumn("s2r", pow((col("ybar") - lit(yest)), 2) * pow(col("N"), 2))
//     val s2r = cdat1.select(sum(col("s2r"))).first.getDouble(0) / (data.count - 1)
//     val mbar = cdat1.select(mean(col("N"))).first.getDouble(0)
//     val yse = s2r / (data.count * mbar * mbar)
//     val mdat = cdat1.select(first("schoolid").alias("dummy"))
//       .withColumn("yest", lit("yest"))
//       .withColumn("yvarFpc", lit("yse"))
//       .withColumn("smpN", lit(data.count))
//       .withColumn("N", lit(allN))

//     def svymean(alpha: Double = 0.05): DataFrame = getEst(mdat, alpha)
//   }

}
