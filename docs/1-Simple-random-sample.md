This page documents how to estimate population quantities using data obtained from a simple
random sample (SRS).

### Demo: agricultural data

The data comes from 300 farms sampled from a population of 3078 farms in the
United States.  The columns `acres92` and  `acres87` represent the number of acres
for each farm in 1992 and 1987, respectively. I create a column (`lt200k`),
which assigns a farm the value 1 if a farm has less than 200,000 acres,
otherwise 0.

The code below imports the soup `Design` object and reads the data.
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
val agdat = spark.read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv(s"$path/agstrat.csv")
  .withColumn("lt200k", when(col("acres92") < 2e5, 1).otherwise(0).alias("lt200k"))
  .withColumn("N", lit(3078.0)) // the number of farms in population
agdat.limit(5).show
```

### SRS class

The `SRS` class is used to estimate population quantities from a SRS. It
requires a `DataFrame` and a `Column` representing the population size
(`popSize`) as arguments. An instance is created as follows:

```scala
val srs = SRS(agdat, popSize = col("N")))
```

There is also weight parameter; if omitted, a default sampling weight is computed
as w = N/n, where  n is the sample size (`smpSize`) and N is the population
size (`popSize`). For this dataset, the sampling weight = 3078/300. You can
override this weight with your own custom weight.

```scala mdoc
val srs = SRS(agdat, popSize = col("N"), weights = lit(3078/300.0))
```
A finite population correction (FPC) factor is also calculated. For confidence
intervals, the default alpha value is 0.05, which you can change with the
`alpha` parameter.

### Estimation

Given an `SRS` instance, you can estimate the mean or a proportion
using `svymean`:

```scala mdoc
val ac92mean = srs.svymean(y = col("acres92"))
ac92mean.show
val ac92prop = srs.svymean(y = col("lt200k"))
ac92prop.show
```

For totals, use `svytotal`:

```scala mdoc
val ac92tot = srs.svytotal(y = col("acres92"))
ac92tot.show
```

To estimate the ratio, use the `svyratio` method:

```scala mdoc 
val acrat = srs.svyratio(num = col("acres92"), den = col("acres87"))
acrat.show
```

Another useful method is `summary`, which gives the aggregated data (mean, total, variance, etc) for a quantity
`y`. 

```scal mdoc
acrat.summary(y = col("acres92"))
```

### Estimation by strata

Estimates for  means (proportions), totals and ratios can be obtained by
strata. To do this, we must pass  arguments to the `strata` and `popSize`
parameters. Previously, the `popSize` was a single value,  `N = 3078`; in the
code below, `popSize` is now the number of farms in each population strata.

```scala mdoc 
// make a new popSize column
val regionCode: Column = typedLit(
    Map("NE" -> 220.0, "NC" -> 1054.0, "S" -> 1382.0, "W" -> 422.0))
val agdat2 = agdat.withColumn("N", regionCode(col("region")))
```
Next, initiate a new instance of the `SRS` class with the `popSize` and `strata` parameters:

```scala mdoc
val tsrs = SRS(agdat2, popSize = col("N"), strata = col("region"))
```

And call `svymean` or `svytotal`:

```scala mdoc
tsrs.svymean(col("acres92")).show
tsrs.svytotal(col("acres92")).show
```

As before, you can override the weights:

```scala mdoc
val wts: Column = typedLit(
Map("NE" -> 2.0, "NC" -> 1.0, "S" -> 9.0, "W" -> 7.0))
val wcol: Column = wts(col("region"))
val tsrs2 = SRS(agdat, popSize = col("N"), weights = wcol, strata = col("region"))
tsrs2.summary(y = col("acres92")).show
```
