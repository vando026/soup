// Project: sampling
// Description: Code to set up offline datasets in test folder
// Date: 17-Mar-2023

import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
import org.apache.spark.sql.{SparkSession, DataFrame, Column}

// val spark = SparkSession
//   .builder()
//   .master("local[*]")
//   .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

val path = Hourly(3, 17, List(16)).custTake(3)

val dat = spark.read.parquet(path)
  .select(sid5.unsigned, justEnded, joinTimeMs)
  .where(shouldProcess)
  .where(joinTimeMs >= 0)
  .withColumn("justEnded", when(col("justEnded") === true, 1).otherwise(0))
  .withColumn("joinTimeSec", col("joinTimeMs")/1000)
  .drop("joinTimeMs")
  .coalesce(1)

// dat.stat.crosstab("justEndedB", "justEnded").show
// No missing on intvNumDroppedFrames and justEnded
// val dat1 = dat
//    .withColumn("check", when(col("justEnded").isNull, 1).otherwise(0))
//    .filter(col("check")  === 1)
//

// dat.count // 10409
// dat.select(sum(col("justEnded")), sum(col("joinTimeSec"))).show()
// 9544 and 18098.669

dat.write.parquet("/mnt/conviva-dev-convivaid0/users/avandormael/data/pbssHourlyTest.parquet")

