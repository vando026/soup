Soup provides an `STRS` class from data obtained from a stratified random
sample. For this demonstration, I use the same data as before, where `region`
is the stratifying variable:

```scala mdoc
import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.soup.Design._

val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate

// use test dataset for demo
val path = "./soup/src/test/data"

val strdat = spark.read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv(s"$path/agstrat.csv")
  .withColumn("lt200k",
    when(col("acres92") < 2e5, 1).otherwise(0))
  .withColumn("N", 
    when(col("region") === "NE", 220.0)
    .when(col("region") === "NC", 1054.0)
    .when(col("region") === "S", 1382.0)
    .when(col("region") === "W", 422.0))
```

We pass the `region` column as an argument to the `strata` parameter. The estimated  means and totals by group are obtained using the `svymeans` and `svytotal` methods:

```scala mdoc
val dstr = STRS(strdat, popSize = col("N"), strata = col("region"))
val ac92mean = dstr.svymean(col("acres92"))
ac92mean.show
val ac92tot = dstr.svytotal(col("acres92"))
ac92tot.show
```
