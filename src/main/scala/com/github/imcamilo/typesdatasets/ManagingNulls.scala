package com.github.imcamilo.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  val spark = SparkSession
    .builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select the first non-null value
  // VERY QUICK PRACTICAL WAY TO SELECT THE FIRST NON NULL VALUE OUT OF A SERIES OF COLUMN
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )
  // .show()
  // Coalesce, does, for every row in the data frame. Coalesce will look in the:
  // Rotten_Tomatoes_Rating first,
  // if its not null, it will fill in that.
  // if its null, it will take a look in IMDB_Rating
  // if IMDB_Rating is not null, it will return that as the result

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls where ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // remove or replace nulls
  // na.drop will remove any rows in the df which any value is null - remove rows containing nulls
  moviesDF.select(col("Title"), col("IMDB_Rating")).na.drop()
  // replace nulls
  moviesDF.na.fill(0, List("Rotten_Tomatoes_Rating", "IMDB_Rating"))
  moviesDF.na.fill(
    Map(
      "IMDB_Rating" -> 0,
      "Rotten_Tomatoes_Rating" -> 10,
      "Director" -> "Unknown"
    )
  )

  // complex operations
  // only available in expr - sql
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // return null if the two values are EQUAL else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if Rotten_Tomatoes_Rating is not null return IMDB_Rating else 0.0
  ).show()

}
