package conviva.soup

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

object Design {

  case class Simple(y: Array[Double], weights: Array[Double], fpc: Array[Double]) {
    val R = org.ddahl.rscala.RClient()
    val lib = "library(survey);"
    val df =  "dat <- data.frame(y = %-, sampwt = %-);"
    val design = "dsrs <- svydesign(id = ~1, weights = ~sampwt, fpc = %-, data = dat);"
    def calcEst(method: String): String =  s"res <- ${method}(~y, dsrs);"
    val cmd = s"$lib $df $design"
    def estimate(method: String = "svymean"): Array[Double] = {
      val result = "round(c(as.numeric(res[1]), sqrt(as.numeric(attributes(res)$var))), 4)"
      val rsnip = s"$cmd ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc)
    }
    def confint(method: String = "svymean"): Array[Double] = {
      val result = s"cc <- confint(res, df=%-); c(cc[1], cc[2])"
      val rsnip = s"$cmd ${calcEst(method)} $result"
      R.evalD1(rsnip, y, weights, fpc, y.length - 1)
    }
  }

}


