package conviva.soup 

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{pow, lit, col}
import conviva.soup.Compute._

object Design {

  case class Simple(data: DataFrame) 
      extends Survey with SVYMean with SVYTotal {
    override val df: Double = n() - 1
    override def smpMVariance(): Column = {
      (col("fpc") * (col("yvar") / col("n"))).alias("yvar")
    }
  }

  case class Stratified(data: DataFrame)  
      extends Survey with SVYMean with SVYTotal {
    override val df: Double = n() - nstrata() 
    override def smpMVariance(): Column = {
      (col("fpc") * pow(col("N_")/N(), 2) * (col("yvar") / col("n"))).alias("yvar")
    }
  }


}
