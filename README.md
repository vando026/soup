<p align="center">
<img src="./media/pot.png" alt="" width="200" >
</p>

<h1 align="center"> conviva-soup</h1>

A Scala library to estimate totals, means, and ratios from simple random sample or stratified random sample designs. 

### Simple random sample designs

For simple random sample (SRS) designs, use the `SRS` class, which requires a `DataFrame` and `Column` representing the sampling weights. 

```scala 
val srs = SRS(agdat, popSize = col("N"), weights = lit(3078/300))
```

To estimate the mean, use the `svymean` method:

```scala mdoc
val ac92mean = srs.svymean(y = col("acres92"))
```

The `svymean` method also works for proportions:

```scala mdoc
val ac92prop = srs.svymean(y = col("lt200k"))
```

To estimate the total use  the `svytotal` method:
```scala mdoc
val ac92tot = srs.svytotal(y = col("acres92"))
ac92tot.show
```

To estimate a ratio, use the `svyratio` method:

```scala mdoc 
val acrat = srs.svyratio(num = col("acres92"), den = col("acres87"))
```


```scala mdoc
val tsrs = SRS(agstrat, popSize = col("N"), strata = col("region"))
```

And call `svymean` or `svytotal`:

```scala mdoc
tsrs.svymean(col("acres92")).show
tsrs.svytotal(col("acres92")).show
```

Soup can also obtain estimates for a group or strata using the `strata`
parameter:

```scala mdoc
val tsrs = SRS(agstrat, popSize = col("N"), strata = col("region"))
tsrs.svymean(col("acres92")).show
tsrs.svytotal(col("acres92")).show
```

Please see the SRS page (https://github.com/Conviva-Internal/soup/wiki/1-Simple-random-sample) for more info. 


### Stratified sampling designs

For a stratified sampling design, use the `STRS` class, which requires a `DataFrame`, a `Column` for the sampling weights, and a `Column` for the strata. As before, the estimated  means and totals by group are obtained using the `svymeans` and `svytotal` methods:

```scala mdoc
val dstr = STRS(strdat, popSize = col("N"), strata = col("region"))
val ac92mean = dstr.svymean(col("acres92"))
val ac92tot = dstr.svytotal(col("acres92"))
```

 
"For large populations it is the size of the sample taken, not the percentage of the population sampled, that determines the precision of the estimator: If your soup is well stirred, you need to taste only one or two spoonfuls to check the seasoning, whether you have made 1 liter or 20 liters of soup." Sharon Lohr - Sampling Design and Analysis (2022).
