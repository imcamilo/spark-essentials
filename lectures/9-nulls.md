### Nullable

when we enforce schema for reading

```scala

// this its the same
val carsDFSchema: StructType = firstDF.schema
val carsDFWithSchema = spark.read
  .format("json")
  .schema(carsDFSchema)
  .load("src/main/resources/data/cars.json")
```

#### Non-nullable columns

- are not constraints.
- are a marker for Spark top optimize for nulls.
- can lead to exception or data errors if broken.

So be careful, use nullable false only if you are pretty sure that the data may never have nulls.