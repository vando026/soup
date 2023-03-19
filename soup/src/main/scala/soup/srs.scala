package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

object Design {
  case class svydesign(y: Array[Double], weights: Array[Double], fpc: Array[Double]) {
    val R = org.ddahl.rscala.RClient()
    val lib = "library(survey);"
    val df =  "dat <- data.frame(y = %-, sampwt = %-);"
    val design = "dsrs <- svydesign(id = ~1, weights = ~sampwt, fpc = %-, data = dat);"
    def calcEst(method: String): String =  s"res <- ${method}(~y, dsrs);"
    val cmd = s"$lib $df $design"
    def estimate(method: String = "svymean"): Array[Double] = {
      val result = "round(c(as.numeric(res[1]), sqrt(as.numeric(attributes(res)$var))), 4)"
      val rsnip = s"""
        | $cmd
        | ${calcEst(method)}
        | $result
      """.stripMargin
      R.evalD1(rsnip, y, weights, fpc)
    }
    def confint(method: String = "svymean"): Array[Double] = {
      val result = s"cc <- confint(res, df=%-); c(cc[1], cc[2])"
      val rsnip = s"""
        | $cmd
        | ${calcEst(method)}
        | $result
      """.stripMargin
      R.evalD1(rsnip, y, weights, fpc, y.length - 1)
    }
  }
}

class SvyDesign(val y: Array[Double], ids: Array[Double],
  stratas: Array[Double], weights: Array[Double], fpc: Array[Double]) {}
object SvyDesign {
  def apply(y: Array[Double], weights: Array[Double], fpc: Array[Double]): String = {
    "this is it" 
  }
  // val df =  "dat <- data.frame(y = %-, strata = %-, sampwt = %-, fpc = %-);"
  // val design = "dstr <- svydesign(id = ~1, strata = ~stratas, weights = ~sampwt, fpc = ~fpc, data = dat);"
}

val tt = svydesign(y = Array(2.0), weights = Array(8.0), fpc = Array(7.9))
tt
