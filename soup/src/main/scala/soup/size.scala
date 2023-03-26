package conviva.soup

// import breeze.linalg._
// import breeze.numerics._
import breeze.stats.distributions._
import scala.math.{pow, sqrt}

object Size {

  /** Calculate the required sample size for a mean using the margin of error (MOE). 
   *  @param e The desired margin of error. 
   *  @param s2 The population variance. 
   *  @param N  The number of units in the finite population.
   *  @param alpha This is `1 - confidence level`. So for a two-sided 95% confidence
   *  interval, specific `alpha = 0.05`.
   *  @example{{{
   *  
   *  }}}
   */
  def nMoe(e: Double, s2: Double, N: Double = Double.PositiveInfinity,
      alpha: Double = 0.05): Int = {
    val zscore = Gaussian(0, 1).inverseCdf(1 - (alpha / 2))
    val num = pow(zscore, 2) * s2
    val denom = pow(e, 2) + (pow(zscore, 2) * (s2 / N))
    (num/denom).ceil.toInt
  }
  
  /** Calculate the sample size for a mean using the coeffient of variation (CV).
   *  @param cv0 Target value of CV of the sample mean. This value must be given as a proportion, ideally between 
   *  0.01 to 0.1.
   *  @param ybar The population mean of target variable.
   *  @param s2 The population variance.  
   *  @param N Number of units in finite population. 
  */
  def nCV(cv0: Double, ybar: Double, s2: Double, 
      N: Double = Double.PositiveInfinity): Int = {
    val cv = sqrt(s2) / ybar
    val n = pow(cv, 2) / (pow(cv0, 2) + (pow(cv, 2) / N))
    n.ceil.toInt
   }

  /** Calculate the required sample size for a proportion using the margin of error (MOE). 
   *  @param e The desired margin of error. 
   *  @param pu The population proportion.
   *  @param alpha This is `1 - confidence level`. So for a two-sided 95% confidence
   *  interval, specific `alpha = 0.05`.
   *  @param N  The number of units in the finite population.
   *  @example{{{
   *  nPropMoe(e = 0.01, alpha = 0.05, pu = 0.04)
   *  }}}
   */
  def nPropMoe(e: Double, pu: Double, N: Double = Double.PositiveInfinity,
      alpha: Double = 0.05): Int = {
    val zscore = Gaussian(0, 1).inverseCdf(1 - (alpha / 2))
    val a = if (N == Double.PositiveInfinity) 1 else N / (N - 1)
    val qu = 1 - pu
    val n = a * pow(zscore, 2) * pu * qu / (pow(e, 2) + pow(zscore, 2) * pu * qu / (N - 1))
    n.ceil.toInt
  }
  
  def nNeyman(strata: List[String], n: Double, Nh: Array[Double], 
      Sh: Array[Double]): Map[String, Double] = {
    val N = Nh.reduce(_ + _).toDouble
    val Wh = Nh.map(_ / N)
    val denom = (Wh, Sh).zipped.map(_ * _).reduce(_ + _)
    val numer = (Wh, Sh).zipped.map(_ * _).map(_ * n)
    val nh = numer.map(_ / denom)
    strata.zip(nh).toMap
  }
}
