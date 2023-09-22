You can also obtain estimates by some grouping variable. Consider the obtaining
estimates for `acres92` by region (the grouping variable).  


```scala 
val strdat_ = spark.read
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

We pass the `region` column as an argument to the `strata` parameter. The estimated  means and totals by group are obtained using the `svymeans` and `svytotal` methods, as before. 

```scala 
val strdat = Summarize(strdat_, y = "acres92", strata = "region").compute
val dstr = Simple(strdat)
val regionMeans  = dstr.svymean()
regionMeans.show
val regionTots = dstr.svytotal()
regionTots.show
```

