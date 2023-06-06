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