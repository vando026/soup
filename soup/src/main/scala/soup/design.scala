package conviva.soup 


object Design {

  import org.apache.spark.sql.{DataFrame, Column}
  import org.apache.spark.sql.functions.{pow, lit, col, sum, mean, first}
  import conviva.soup.Compute._

  case class Simple(data: DataFrame) extends Survey  {
    val meanData = data.select(data.col("*"), smpMean, smpMVar)
    def svymean(alpha: Double = 0.05): DataFrame = getEst(meanData, alpha)
    val totData = data.select(data.col("*"), smpTotal, smpTVar)
    def svytotal(alpha: Double = 0.05): DataFrame =  getEst(totData, alpha)
  }


  case class Stratified(data: DataFrame) extends Survey {
    override def calcDf: Column = col("smpN") - lit(data.count)
    val bigN = data.select(sum("N")).first.getDouble(0)
    override def smpMean(): Column = (col("ybar") * col("N") / lit(bigN)).alias("yest")
    override def smpMVar(): Column = {
      (col("fpc") * pow(col("N") / lit(bigN), 2) * (col("yvar") / col("smpN"))).alias("yvarFpc")
    }
    val vars = List("yest", "yvarFpc", "smpN", "N").map(i => sum(col(i)).alias(i))
    val meanData = data.select(smpMean, smpMVar, col("smpN"), col("N"))
      .agg(vars.head, vars.tail:_*)
    def svymean(alpha: Double = 0.05): DataFrame = getEst(meanData, alpha)
    val totData = data.select(smpTotal, smpTVar, col("smpN"), col("N"))
      .agg(vars.head, vars.tail:_*)
    def svytotal(alpha: Double = 0.05): DataFrame =  getEst(totData, alpha)
  }


case class Clust2Stage(data: DataFrame) extends Survey {
  import org.apache.spark.sql.expressions.Window
  val dw = Window.partitionBy(lit(1))
  // val n = data.count
  override def calcDf = lit(data.count - 1)
  // calculate ytot and yvar

  // see table 5.7 for calc in Lohr
  val allN = data.select(sum(col("N"))).first.getLong(0).toDouble
  val smallN = data.select(sum(col("smpN"))).first.getLong(0).toDouble
  // col 5
  val cdat = data
    .withColumn("ytot", col("ybar") * col("N"))
  val ytotSum = cdat.select(sum(col("ytot"))).first.getDouble(0)
  val yest = ytotSum / allN
  val cdat1 = cdat.withColumn("s2r", pow((col("ybar") - lit(yest)), 2) * pow(col("N"), 2))
  val s2r = cdat1.select(sum(col("s2r"))).first.getDouble(0) / (data.count - 1)
  val mbar = cdat1.select(mean(col("N"))).first.getDouble(0)
  val yse = s2r / (data.count * mbar * mbar)
  val mdat = cdat1.select(first("schoolid").alias("dummy"))
    .withColumn("yest", lit("yest"))
    .withColumn("yvarFpc", lit("yse"))
    .withColumn("smpN", lit(data.count))
    .withColumn("N", lit(allN))

  def svymean(alpha: Double = 0.05): DataFrame = getEst(mdat, alpha)
}




}
