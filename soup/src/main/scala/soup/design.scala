package conviva.soup 


object Design {

  import org.apache.spark.sql.{DataFrame, Column}
  import org.apache.spark.sql.functions.{pow, lit, col, sum}
  import conviva.soup.Compute._

  case class Simple(data: DataFrame) extends Survey  {
    val smpStats = data.select(col(data.columns(0)), smpMean, smpVar, col("smpN"))
    def svymean(alpha: Double = 0.05): DataFrame = getEst(smpStats, alpha)
  }

  // case class Stratified(data: DataFrame) extends Survey with SVYMean with SVYTotal {
  //   val df: Double = n() - nstrata() 
  //   override def smpMean(): Column = (col("ybar") * col("N_") / col("allN").alias("yest"))
  //   override def smpMVariance(): Column = {
  //     (col("fpc") * pow(col("N_")/N(), 2) * (col("yvar") / col("n"))).alias("yvar")
  //   }
  // }


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
