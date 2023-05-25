package com.github.imcamilo.dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}

object ExpressionsAndColumnsExercise extends App {

  val spark = SparkSession
    .builder()
    .appName("DF Expression and Columns Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  /*
   * Exercises
   *
   * 1. Read movies df and select two columns of your choice.
   * 2. Create a new DF summing up the total profit of the movie: US_Gross + Worldwide_Gross + US_DVD_Sales
   * 3. select all the comedy movies: Major_Genre: "Comedy" and IMDB_Rating above 6
   *
   * Use as many versions as possible
   *
   * */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  val movieTitle = moviesDF.col("Title")
  val movieUSGross = moviesDF.col("US_Gross")
  val movieWorldwideGross = moviesDF.col("Worldwide_Gross")
  val movieUSDVDSales = moviesDF.col("US_DVD_Sales")
  val movieIMDBRating = moviesDF.col("IMDB_Rating")
  val movieMajorGenre = moviesDF.col("Major_Genre")

  val reducedMoviesDF: DataFrame =
    moviesDF.select(movieTitle, movieUSGross, movieWorldwideGross, movieUSDVDSales, movieIMDBRating, movieMajorGenre)
  reducedMoviesDF.show()

  // 2
  // US_Gross + Worldwide_Gross + US_DVD_Sales
  import org.apache.spark.sql.functions._
  val moviesProfit =
    moviesDF.select(
      col("Title"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      col("US_DVD_Sales"),
      (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross") // avoiding dvd sales for nulls
    )
  moviesProfit.show()

  val moviesProfit2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )
  moviesProfit2.show()

  val moviesProfit3 = moviesDF
    .select("Title", "US_Gross", "Worldwide_Gross", "US_DVD_Sales")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  // 3
  val comedyMoviesDF = reducedMoviesDF
    .filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
    .show()

  val comedyMovies2DF = moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
    .show()

  val comedyMovies3DF = moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)
    .show()

  val comedyMovies4DF = moviesDF
    .select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    .show()
}
