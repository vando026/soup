The second demo uses Conviva 1-minute data. Consider this to be the census data. 

```scala mdoc
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import conviva.soup.Design._

val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate

// use test dataset for demo
val path = "./soup/src/test/data"

val pdat = spark.read.parquet(s"$path/tlb_ssm101_23_06_19_20.parquet")
pdat.columns
```

First calculate the population quantities from the census data. 

```scala mdoc
// Calculate population quantities
val N = pdat.count.toDouble

val popVars = pdat.agg(
    sum(col("isEnded").cast("integer")).alias("isEnded"),
    sum(col("life_hasJoined").cast("integer")).alias("hasJoined"),
    sum(col("life_isExitDueToCIR").cast("integer")).alias("isExitDueToCIR"),
    sum(col("life_exitDuringPreRoll").cast("integer")).alias("exitDuringPreRoll"),
    sum(col("life_heartbeatCount")).alias("heartbeatCount")
)
popVars.show
``` 

Now, we need to impose some sampling strategy on this data. Sessions were randomly assigned to clusterId using a uniform distribution. Therefore, we can simulate a SRS design by dropping `custId = 2` and using the remaining two clusters to estimate the population quantities. 

```scala mdoc
// drop cluster 2
val smpdat = pdat.where(col("cluId") =!= 2)
// add column with popSize N (required)
val smpdat1 = smpdat
    .withColumn("N", lit(N))
    .withColumn("isEnded", col("isEnded").cast("integer"))
    .withColumn("hasJoined", col("life_hasJoined").cast("integer"))
``` 

We can use  the `SRS` class for estimation. The
below code estimates the  number of `isEnded` and `hasJoined` sessions from the
sample data. 

```scala mdoc
val srs = SRS(smpdat1, popSize = col("N"))
srs.svytotal(col("isEnded")).show
srs.svytotal(col("hasJoined")).show
```


