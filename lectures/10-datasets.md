### Datasets

Are essentially typed dataframes, distributed collection of JVM objects

#### Util when:

- we want to maintain typed information
- we want to clean and concise code
- our filter and transformations are hard to express in DF or SQL

Datasets are essentially a more powerful version of dataframes because we also have types

#### Avoid when:

- performance is critical. Spark cant optimize transformations.

DataFrames are datasets as well. Because, DataFrame it's an alias of Dataset[Row]. Row is a very generic type.

```scala
  type DataFrame = Dataset[Row]
```

#### Joining and grouping