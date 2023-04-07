package conviva.soup 

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{pow, lit, col}
import conviva.soup.Compute._
import org.apache.commons.math3.distribution.TDistribution

object Design {

  def getTstat(df: Double, alpha: Double): Double = {
    new TDistribution(df).inverseCumulativeProbability(1 - (alpha/2))
  }
  case class Simple(data: DataFrame, alpha: Double = 0.05) 
      extends Survey with SVYMean with SVYTotal {
    val df: Double = n() - 1
    override val tstat = getTstat(df, alpha)
    override def smpMVariance(): Column = {
      (col("fpc") * (col("yvar") / col("n"))).alias("yvar")
    }
  }

  case class Stratified(data: DataFrame, alpha: Double = 0.05)  
      extends Survey with SVYMean with SVYTotal {
    val df: Double = n() - nstrata() 
    override val tstat = getTstat(df, alpha)
    override def smpMVariance(): Column = {
      (col("fpc") * pow(col("N_")/N(), 2) * (col("yvar") / col("n"))).alias("yvar")
    }
  }


}
