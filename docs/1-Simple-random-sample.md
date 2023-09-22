## Simple Random Sample: Estimation

This page demonstrates how to estimate population quantities for a simple
random sample (SRS) using the `Simple` object from `soup`. 

### Agricultural data

This first demo shows how to estimate means, proportions and totals from a
simple random sample (SRS) design using agricultural data.  To estimate means
and totals, I use the  column `acres92`, which  is the number of acres for 300
farms randomly sampled in 1992. To estimate proportions,  I create a column
where acres > 200,000 are coded as a  1 otherwise 0. The total number of farms
in the census is 3078.



```scala mdoc 

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.soup.Design._

val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate

// use test dataset for demo
val path = "./soup/src/test/data"

// read in the data
val sdat = spark.read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv(s"$path/agsrs.csv")
  .select(
    col("acres92"),
    when(col("acres92") < 2e5, 1).otherwise(0).alias("lt200k")
  )
  .withColumn("N", lit(3078.0))
sdat.show
```

The `SRS` class takes a `DataFrame` and a `Column` representing the population size (`popSize`) as arguments. 

```scala mdoc 
SRS(sdat, popSize = col("N"))
```

The sampling weights (w = N/n) are computed using the sample size of the data (n = `smpSize`) and the population size (N = `popSize`). You can override this computation by providing your own weight as an argument, which must be a constant `lit(weight)`.


```scala mdoc 
SRS(sdat, popSize = col("N"), weights = lit(3078/300))
```

The 
variance of `acres92`  with the finite population correction (FPC) factor. You
can specify any column name, but you must make sure you pass a column called
`N`, which represents the population total.

```scala mdoc
val srs = SRS(sdat, popSize = col("N"))
```

The `Simple` class is used to obtain estimates from a SRS design. To
compute either the estimated mean or total, use the `svymean` and `svytotal` methods respectively. 

```scala mdoc
val acr92mean = srs.svymean(y = col("acres92"))
acr92mean.show
val acr92total = srs.svytotal(y = col("acres92"))
acr92total.show
```

The methods also work for proportions.

```scala mdoc
val ltsrs = SRS(sdat, popSize = col("N"))
// ltsrs.show
val lt200mean = ltsrs.svymean(y = col("lt200k"))
lt200mean.show
val lt200total = ltsrs.svytotal(y = col("lt200k"))
lt200total.show
```

Note that you can specify the alpha value to calculate confidence intervals as
follows (the default alpha is 0.05).

```scala 
val t3 = ltsrs.svymean(0.1)
t3.show
```

