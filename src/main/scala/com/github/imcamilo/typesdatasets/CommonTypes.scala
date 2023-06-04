package com.github.imcamilo.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession
    .builder()
    .appName("Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // 1 ADDING A PLAIN_VALUE TO A DF
  // lit will work with string, numbers, booleans
  moviesDF
    .select(col("Title"), lit(8766).as("plain_value"))
  // .show()

  // 2 BOOLEANS
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(dramaFilter)
  // multiple ways of filtering

  // good_movie will be evaluated true or false
  val moviesWithGoodnessFlags = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // filter on boolean column name
  moviesWithGoodnessFlags.where("good_movie") // where(col("good_movie") === "true")
  // negations
  moviesWithGoodnessFlags
    .where(not(col("good_movie"))) // where(col("good_movie") !== "true")

  // 3 NUMBERS

  // math operators
  // this should work well because are numeric fields, if not, spark will throw an exception
  val floatType = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // util for data science
  // correlation = a number between -1 and 1
  // (between columns) for example the correlations between Rotten_Tomatoes_Rating and IMDB_Rating, closer to 1 is better.
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an ACTION

  // 4 STRINGS
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // we have initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))
  // in case you need to chain volskwagen and vw

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("regex_extract")
    )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")
  // vwDF.show()

  val vwReplaceDF = vwDF
    .select(
      col("Name"),
      regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
    )
  vwReplaceDF.show()

}
