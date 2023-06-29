package conviva.soup 


object Design {

  import org.apache.spark.sql.{DataFrame, Column}
  import org.apache.spark.sql.functions.{pow, lit, col, sum}
  import conviva.soup.Compute._

  case class Simple(data: DataFrame) extends Survey  {
    val meanData = data.select(data.col("*"), smpMean, smpMVar)
    def svymean(alpha: Double = 0.05): DataFrame = getEst(meanData, alpha)
    val totData = data.select(data.col("*"), smpTotal, smpTVar)
    def svytotal(alpha: Double = 0.05): DataFrame =  getEst(totData, alpha)
  }

  case class Stratified(data: DataFrame) extends Survey {
    override def calcDf: Column = col("smpN") - lit(data.count)
    val bigN = data.select(sum("N")).first.getDouble(0)
    override def smpMean(): Column = (col("ybar") * col("N") / lit(bigN)).alias("yest")
    override def smpMVar(): Column = {
      (col("fpc") * pow(col("N") / lit(bigN), 2) * (col("yvar") / col("smpN"))).alias("yvarFpc")
    }
    val vars = List("yest", "yvarFpc", "smpN", "N").map(i => sum(col(i)).alias(i))
    val meanData = data.select(smpMean, smpMVar, col("smpN"), col("N"))
      .agg(vars.head, vars.tail:_*)
    def svymean(alpha: Double = 0.05): DataFrame = getEst(meanData, alpha)
    val totData = data.select(smpTotal, smpTVar, col("smpN"), col("N"))
      .agg(vars.head, vars.tail:_*)
    def svytotal(alpha: Double = 0.05): DataFrame =  getEst(totData, alpha)
  }


// case class Clust2Stage(data: DataFrame) {
//   import org.apache.commons.math3.distribution.TDistribution
//   import org.apache.spark.sql.{SparkSession, DataFrame, Column}
//   import org.apache.spark.sql.functions._
//   import org.apache.commons.math3.distribution.TDistribution

//   private def getEst(yest: Double, yvar: Double, df: Double,  
//       alpha: Double): Map[String, Double] = {
//     val tstat = new TDistribution(df)
//       .inverseCumulativeProbability(1 - (alpha/2))
//     lazy val yse = math.sqrt(yvar)
//     lazy val width = tstat * yse
//     lazy val lb = yest - width
//     lazy val ub = yest + width
//     Map("yest" -> yest, "yvar" -> yvar, "yse" -> yse, "lb" -> lb, "ub" -> ub)
//   }

//   // calculate ytot and yvar
//   val __dat = data.withColumn("ytot", col("ybar") * col("Mi_"))

//   val n = __dat.count
//   val df = n - 1

//   val yest = __dat.select(sum("ytot")/sum("Mi_"))
//     .first.getDouble(0)

//   // between cluster variance
//   val yssb = __dat
//     .withColumn("s2r",
//       pow((col("ybar") - lit(yest)), 2) * pow(col("Mi_"), 2))
//     .agg(sum("s2r"))
//     .first.getDouble(0)

//   val s2r = yssb * (1.0 / df)
//   val mbar = __dat.agg(mean("Mi_")).first.getDouble(0)
//   val yvwr = s2r / (n *  mbar * mbar)

//   def svymean(alpha: Double = 0.05): Map[String, Double] =
//     getEst(yest, yvwr, df, alpha)
// }




}
