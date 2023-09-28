This page demonstrates how to estimate population quantities using data from a simple
random sample (SRS).

### Demo: agricultural data

For demonstration, I use data collected from a simple random sample of 300
farms from a population of 3078 farms in the United States.  The column
`acres92` and  `acres87` are the number of acres each farm has for 1992 and
1987, respectively. I create a third column (`lt200k`), which assigns a farm
the value 1 if the farm has greater than 200,000 acres, otherwise 0.

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
  .csv(s"$path/agsrs2.csv")
  .select(
    col("acres92"),
    col("acres87"),
    when(col("acres92") < 2e5, 1).otherwise(0).alias("lt200k")
  )
  .withColumn("N", lit(3078.0))
agdat.limit(5).show
```

### Estimation

This section shows how to estimate means (proportions), totals, and ratios
using the `SRS` class.  The `SRS` class requires a `DataFrame` and a `Column`
representing the population size (`popSize`) as arguments. 

```scala mdoc
val srs = SRS(agdat, popSize = col("N"), weights = lit(3078/300))
```

The sampling weights (w = N/n) are computed using the sample size of the data (n = `smpSize`) and the population size (N = `popSize`). You can override this computation by providing your own weight as an argument, which must be a `Column` class. A finite population correction (FPC) factor will also be calculated. For confidence intervals, shown later, the default alpha value is 0.05, which you can change with the `alpha` parameter.


Given an `SRS` instance, you can estimate the mean of a quantity using `svymean`:

```scala mdoc
val ac92mean = srs.svymean(y = col("acres92"))
ac92mean.show
```
or the total using `svytotal`:
```scala mdoc
val ac92tot = srs.svytotal(y = col("acres92"))
ac92tot.show
```

The `svymean` method also works for proportions:

```scala mdoc
val ac92prop = srs.svymean(y = col("lt200k"))
ac92prop.show
```

To estimate the ratio, use the `svyratio` method. 

```scala mdoc 
val acrat = srs.svyratio(num = col("acres92"), den = col("acres87"))
acrat.show
```

### Estimation by strata

To estimate means (proportions) or totals by strata, pass an argument to the
`strata` parameter. To demonstrate, I read in agricultural data stratified by
region. 

```scala mdoc 
val agstrat = spark.read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv(s"$path/agstrat.csv")
  .withColumn("N", 
    when(col("region") === "NE", 220.0)
    .when(col("region") === "NC", 1054.0)
    .when(col("region") === "S", 1382.0)
    .when(col("region") === "W", 422.0))
```

Now pass the argument to `strata`:

```scala mdoc
val tsrs = SRS(agstrat, popSize = col("N"), strata = col("region"))
```

And call `svymean` or `svytotal`:

```scala mdoc
tsrs.svymean(col("acres92")).show
tsrs.svytotal(col("acres92")).show
```

