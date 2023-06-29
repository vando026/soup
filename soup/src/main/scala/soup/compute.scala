package conviva.soup


object Compute {

  import org.apache.commons.math3.distribution.TDistribution
  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._

  /** Prepare the data for estimation by taking mean and variance of y with n
   *  and N. 
   *  @param data The input dataframe. 
   *  @param y The quantity or variable to estimate.
   *  */
  case class Summarize(dat: DataFrame, y: String, strata: String = "") {
    def compute(): DataFrame = {
      assert(dat.columns.contains("N"), "Column called N required for population total.")
      val byVar = strata match {
        case x if x == "" => lit(1)
        case _ => col(strata)
      }
      dat.groupBy(byVar).agg(
        mean(col(y)).alias("ybar"),
        variance(y).alias("yvar"),
        count("*").cast("double").alias("smpN"),
        first("N").alias("N")
      )
      .withColumn("fpc", lit(1) - (col("smpN") / col("N")))
    }
  }

  trait Survey {

    /** Get the final estimates from sampled data with standard error and
     *  confidence intervals.
     *  @param data A dataset processed from `Summarized`.
     *  */ 
    val data: DataFrame
    def calcDf(): Column = col("smpN") - lit(1)
    def smpMean(): Column = col("ybar").alias("yest")
    def smpVar(): Column = {
      (col("fpc") * (col("yvar") / col("smpN"))).alias("yvar")
    }
    def tstat(alpha: Double) = udf((col: Double) => {
      new TDistribution(col).inverseCumulativeProbability(1 - (alpha / 2))
    })

    def getEst(data: DataFrame, alpha: Double): DataFrame = {
       data
        .withColumn("yse",  sqrt(col("yvar")))
        .withColumn("df", calcDf)
        .withColumn("tstat", tstat(alpha)(col("df")))
        .withColumn("width", col("tstat") * col("yse"))
        .withColumn("lb", col("yest") - col("width"))
        .withColumn("ub", col("yest") + col("width"))
        .drop("width")
    }

    // def summary(): DataFrame = data

    /** Calculate the coefficient of variation. */
    // def cv_(yest: Column, yse: Column): Column = yse / yest
  
    /** Return the population size. */
    // def N(): Double = data.select(col("N_"))

    /** Return the sample size. */
    // def n(): Double = data.select(col("n"))

    /** Return the number of strata. */
    // def nstrata(): Long =  data.count

  }

  // trait SVYMean extends Survey {
  //   def smpMean(): Column = (col("ybar") * col("N_") / N).alias("yest")
  //   def smpMVariance: Column
  //   val __mdat = data.select(col(data.columns(1)), smpMean, smpMVariance)
  //   /** Calculate the survey mean, with standerd error and confidence intervals.
  //    *  @param alpha The default value is 0.05 for 95% conidence intervals. 
  //    *  */
  //   def svymean(alpha: Double = 0.05): DataFrame = getEst(__mdat, df, alpha)
  // }

  // trait SVYTotal extends Survey {
  //   def smpTotal(): Column = (col("ybar") * col("N_")).alias("yest")
  //   def smpTVariance(): Column = 
  //     (col("fpc") * pow(col("N_"), 2) * (col("yvar") / col("n"))).alias("yvar")
  //   val __tdat = data.select(col(data.columns(1)), smpTotal, smpTVariance)
  //     // .agg(sum("yest").alias("yest"), sum("yvar").alias("yvar"))
  //   /** Calculate the survey mean, with standerd error and confidence intervals.
  //    *  @param alpha The default value is 0.05 for 95% conidence intervals. 
  //    *  */
  //   def svytotal(alpha: Double = 0.05): DataFrame =  getEst(__tdat, df, alpha)
  // }

}

// def test(s: String = "") = {
//   s match {
//     case x if x == "" => "yes"
//     case _ => s
//   }
// }
// test()
