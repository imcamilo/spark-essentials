### RDD

Resilient Distributed Datasets

Let's start:

- How to read from external sources?.
- How to convert to/from DataFrames and Datasets?.
- Difference between RDDs/DataFrames/DataSets.

#### Resilient Distributed Datasets

Are essentially distributed type of collections of JVM objects. (Similar to Datasets).
The difference is that RDD are actually the underlying first citizens of Spark:
**The "first citizens" of Spark: All higher-level APIs reduce to RDDs.**

So Why would we want to use RDD?:

**Pros, Can be highly optimized:**

- Partitions can be controlled. (DF/DS cant do that)
- Order of elements can be controlled.
- Order of operations matter for performance. (big impact).

**Cons, Hard to work with:**

- For complex operations, need to know the internals of Spark.
- Poor APIs for quick data processing.

For 99% of operations, use DataFrames and Datasets APIs.

### RDDs vs ~~DataFrames~~ vs DataSets

Why the data frame is crossed... It's because DataFrame It's a DataSet of Rows.

#### In common

- DataFrame = DataSet[Row].
- In common API: **map**, **flatMap**, **filter**, **take**, **reduce**, etc.
- **union**, **count** and **distinct**.
- **groupBy**, **sortBy**.

#### RDDs over DataSets

- Partition control: **repartition**, **coalesce**, **partitioner**, **zipPartitions**, **mapPartitions**.
- Operation control: **checkpoint**, **isCheckpointed**, **localCheckpointed**, **cache**.
- Storage control: **cache**, **getStorageLevel**, **persist**.

#### DataSets over RDDs

- **select** and **join**.
- Spark planning optimizations before running code. (For these reasons we wouuld use DF/DS APIs)

#### Takeaways

Turn a regular collection into a RDD.

```scala
val numbers = 1 to 1000000
val numbersRDD: RDD[Int] = sc.parallelize(numbers)
```

Read from file (need to process lines).

```scala
val numbersRDD = sc.textFile("path/to/your/file").map(/**/)
```

DataSet to RDD. (All datasets can access underlying RDDs)

```scala
val numbersRDD2 = numbersDS.rdd
```

RDD to High-Level.

```scala
val numbersDF = numbersRDD2.toDF("column1", "column2")
val numbersDS = spark.createDataset(numbersRDD)
```
