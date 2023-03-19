package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

object Design {

  val R = org.ddahl.rscala.RClient()

  trait Design {
    val lib = "library(survey);"
    val y: Array[Double]
    val weights: Array[Double]
    val fpc: Array[Double]
    val df: String
    val design:String 
    def calcEst(method: String): String =  s"res <- ${method}(~y, design);"
    val cmd = s"$lib $df $design"
    def estimate(method: String = "svymean"): Array[Double] = {
      val result = "round(c(as.numeric(res[1]), sqrt(as.numeric(attributes(res)$var))), 4)"
      val rsnip = s"$lib $df $design ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc)
    }
    def confint(method: String = "svymean"): Array[Double] = {
      val result = s"cc <- confint(res, df=%-); c(cc[1], cc[2])"
      val rsnip = s"$lib $df $design ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc, y.length - 1)
    }
  }

  case class SimpleDesign(y: Array[Double], weights: Array[Double], fpc: Array[Double]) 
      extends Design {
    override val df =  "df <- data.frame(y = %-, sampwt = %-);"
    override val design = "design <- svydesign(id = ~1, weights = ~sampwt, fpc = %-, data = df);"
  }

val dsrs = SimpleDesign(acres92, weights = pweights, fpc = fpc )
dsrs.estimate("svymean")

}

class SvyDesign(y: Array[Double], weights: Array[Double], fpc: Array[Double]) {
    // var y = Array(1.0)
    // var weights = Array(1.0)
    // var fpc = Array(1.0)
    val lib = "library(survey);"
    var df = ""
    var design = ""
    def calcEst(method: String): String =  s"res <- ${method}(~y, des);"
    def estimate(method: String = "svymean"): Array[Double] = {
      val result = "round(c(as.numeric(res[1]), sqrt(as.numeric(attributes(res)$var))), 4)"
      val rsnip = s"$lib $df $design ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc)
    }
    def confint(method: String = "svymean"): Array[Double] = {
      val result = s"cc <- confint(res, df=%-); c(cc[1], cc[2])"
      val rsnip = s"$lib $df $design ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc, y.length - 1)
    }
}
object SvyDesign {
  def apply(y: Array[Double], weights: Array[Double], fpc: Array[Double]): SvyDesign = {
    var p = new SvyDesign(y, weights, fpc)
    p.df =  "df <- data.frame(y = %-, sampwt = %-);"
    p.design = "des <- svydesign(id = ~1, weights = ~sampwt, fpc = %-, data = df);"
    p
  }
  def apply(y: Array[Double], strata: Array[Double], weights: Array[Double], fpc: Array[Double]): SvyDesign = {
    var p = new SvyDesign(y, weights, fpc)
    p
    p.df =  "df <- data.frame(y = %-, sampwt = %-, stratas = %-);"
    p.design = "des <- svydesign(id = ~1, strata=~stratas, weights = ~sampwt, fpc = %-, data = df);"
    p
  }
}


val dsrs = SvyDesign(acres92, weights = pweights, fpc = fpc )
dsrs.estimate("svymean")
val tt = svydesign(y = Array(2.0), weights = Array(8.0), fpc = Array(7.9))
tt
