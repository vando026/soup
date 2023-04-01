package conviva.soup

import conviva.surgeon.Paths._
import conviva.soup.sampler._
import org.apache.spark.sql.{DataFrame, Column, Row}
import conviva.surgeon.PbSS._
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.sql.functions.{
  col, pow, sqrt, lit, sum, count, variance, first, mean
}

object simple {


  case class Simple2(data: DataFrame, y: Column, N: Double, alpha: Double = 0.05) {

    val sdat = data.agg(
      mean(y).alias("ybar"),
      variance(y).alias("yvar"),
      count("*").cast("double").alias("nsize")
    )
    
    val mn = sdat.select(col("ybar")).collect()(0).getDouble(0)
    val s2 = sdat.select(col("yvar")).collect()(0).getDouble(0)
    val n = sdat.select(col("nsize")).collect()(0).getDouble(0)
    val weight = n / N
    val fpc = 1 - (n / N)
    val se = math.sqrt(fpc * s2/n)
    val tstat = new TDistribution(n - 1)
      .inverseCumulativeProbability(1 - (alpha/2))

    def summary(): DataFrame = {
      sdat
    }

    def confint(mn: Double, se: Double): Array[Double] = {
      val width = tstat * se
      Array(mn - width, mn + width)
    }

    def svymean(): Map[String, Double] = {
      val ci = confint(mn, se)
      Map("mean" -> mn, "se" -> se, "lb" -> ci(0), "ub" -> ci(1))
    }

    def svytotal(): Map[String, Double] = {
      val ci = confint(mn, se).map(_ * N)
      Map("total" -> N * mn, "se" -> N * se, "lb" -> ci(0), "ub" -> ci(1))
    }

    def cv(): Double = {
      se / mn
    }
  }


}
