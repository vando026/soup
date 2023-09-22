### Conviva data

The second demo uses Conviva 1 minute data for June 2023, 19:20.
Consider this to be the census data. 

```scala 
val pdat = spark.read.parquet(s"$path/tlb_ssm101_23_06_19_20.parquet")
pdat.columns
```

First calculate the population quantities from the census data. 

```scala  
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

```scala  
// drop cluster 2
val smpdat = pdat.where(col("cluId") =!= 2)
// add column with popSize N (required)
val smpdat1 = smpdat
    .withColumn("N", lit(N))
    .withColumn("isEnded", col("isEnded").cast("integer"))
    .withColumn("hasJoined", col("life_hasJoined").cast("integer"))
``` 

We are ready to use the `Summarize` and `Simple` classes for estimation. The
below code estimates the  number of `isEnded` and `hasJoined` sessions from the
sample data. 

```scala 
val ended = Summarize(smpdat1, y = "isEnded").compute
ended.show

val smpEnded = Simple(ended)
smpEnded.svytotal()
```

```scala 
val joined = Summarize(smpdat1, y = "hasJoined").compute
joined.show
val smpJoined = Simple(joined)
smpJoined.svytotal()
```

