package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

object designer {

  val R = org.ddahl.rscala.RClient()

  case class Simple(y: Array[Double], weights: Array[Double], fpc: Array[Double]) {
    val lib = "library(survey);"
    val dat =  "dat <- data.frame(y = %-, sampwt = %-);"
    val design = "dsrs <- svydesign(id = ~1, weights = ~sampwt, fpc = %-, data = dat);"
    val df = y.length - 1
    def calcEst(method: String): String =  s"res <- ${method}(~y, dsrs);"
    val cmd = s"$lib $dat $design"
    def estimate(method: String = "svymean"): Array[Double] = {
      val result = "round(c(as.numeric(res[1]), sqrt(as.numeric(attributes(res)$var))), 4)"
      val rsnip = s"$cmd ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc)
    }
    def confint(method: String = "svymean"): Array[Double] = {
      val result = s"cc <- confint(res, df=%-); c(cc[1], cc[2])"
      val rsnip = s"$cmd ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc, df)
    }
  }

  case class Stratified(y: Array[Double], strata: Array[String], 
      weights: Array[Double], fpc: Array[Double]) {
    val lib = "library(survey);"
    val dat =  "dat <- data.frame(y = %-, sampwt = %-, fpc = %-, strata = %-);"
    val design = "dstr <- svydesign(id = ~1, strata=~strata, weights = ~sampwt, fpc = ~fpc, data = dat);"
    def calcEst(method: String): String =  s"res <- ${method}(~y, dstr);"
    val cmd = s"$lib $dat $design"
    val df = y.length - strata.distinct.length // df = (sampsize - strata size)
    def estimate(method: String = "svymean"): Array[Double] = {
      val result = "round(c(as.numeric(res[1]), sqrt(as.numeric(attributes(res)$var))), 4)"
      val rsnip = s"$cmd ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc, strata)
    }
    def confint(method: String = "svymean"): Array[Double] = {
      val result = s"cc <- confint(res, df=%-); c(cc[1], cc[2])"
      val rsnip = s"$cmd ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc, strata, df)
    }
  }


}


