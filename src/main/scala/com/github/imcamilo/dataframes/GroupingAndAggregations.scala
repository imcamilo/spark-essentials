package com.github.imcamilo.dataframes

import org.apache.spark.sql.SparkSession

object GroupingAndAggregations extends App {

  val spark = SparkSession
    .builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // COUNTING

  // SELECT ALL THE DIFF GENRES IN JSON
  import org.apache.spark.sql.functions.{col, count}
  val genresCount = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  val genresCount2 = moviesDF.selectExpr("count(Major_Genre)") // its the same

  // COUNTING ALL
  val allCount = moviesDF.select(count("*")) // count all the rows, and it will INCLUDE nulls
  // allCount.show()
  // genresCount.show()

  // COUNT DISTINCT VALUES
  import org.apache.spark.sql.functions.countDistinct
  val genresCountDistinct = moviesDF.select(countDistinct(col("Major_Genre")))
  // genresCountDistinct.show()

  // APPROXIMATE COUNT
  import org.apache.spark.sql.functions.approx_count_distinct
  val genresCountApprox = moviesDF.select(approx_count_distinct(col("Major_Genre")))
  // genresCountApprox.show()

  // MIN AND MAX
  import org.apache.spark.sql.functions.min
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  val minRatingDF2 = moviesDF.selectExpr("min(IMDB_Rating)")
  // minRatingDF.show()
  // minRatingDF2.show()

  // SUM AND AVG
  import org.apache.spark.sql.functions.sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  import org.apache.spark.sql.functions.avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // FOR DATA SCIENCE

  // its util to have standard deviation and means so
  import org.apache.spark.sql.functions.{mean, stddev}
  val utilsForDS = moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )
  utilsForDS.show()

  // standard deviations means how close or how far the different values of Rotten Tomatoes Rating are to the mean.
  // a lower standard deviation will mean that the values are closer to the average
  // a higher standard deviation will mean that the values are more spread out over a wide spectrum

}
