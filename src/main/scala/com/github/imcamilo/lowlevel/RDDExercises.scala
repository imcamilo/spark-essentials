package com.github.imcamilo.lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

object RDDExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("Introduction to RDD")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
  1. Read the movies.json as an RDD.
  2. Show the distinct genres as an RDD.
  3. Select all the movies in the drama genre with IMDB rating > 6.
  4. Show the average rating of movies by genre.
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    )
  // .where(col("genre").isNotNull and col("genre").isNotNull)

  case class Movie(title: String, genre: Option[String], rating: Option[Double])

  import spark.implicits._
  val moviesDS = moviesDF.as[Movie]

  // 1
  val moviesRDD = moviesDS.rdd

  // 2
  val genreCountRDD = moviesRDD.filter(_.genre.nonEmpty).map(_.genre.get).distinct()
  /*
   12 genres
   Concert/Performance
   Western
   Musical
   Horror
   Romantic Comedy
   Comedy
   Black Comedy
   Documentary
   Adventure
   Drama
   Thriller/Suspense
   Action
   */

  // 3 - 591
  val goodDramaMoviesRDD: RDD[Movie] = moviesRDD
    .filter(_.genre == Some("Drama"))
    .filter(_.rating.getOrElse(0d) > 6)
  goodDramaMoviesRDD.toDF().show()

  // 4
  val averageRatingForMoviesGenreRDD: RDD[(String, Iterable[Movie])] =
    moviesRDD.filter(_.genre.nonEmpty).groupBy(_.genre.get)

  val avegareByGenreRDD: RDD[(String, Double)] = averageRatingForMoviesGenreRDD.map(currentGenre => {
    val moviesRating: List[Double] = currentGenre._2.map(_.rating.getOrElse(0d)).toList
    val avg = moviesRating.sum / moviesRating.size
    (currentGenre._1, avg)
  })

  avegareByGenreRDD.toDF().show()
  moviesRDD.toDF().groupBy(col("genre")).avg("rating").show()
  /*
  +-------------------+------------------+
  |                 _1|                _2|
  +-------------------+------------------+
  |Concert/Performance|5.0600000000000005|
  |            Western| 6.652777777777777|
  |            Musical| 6.083018867924529|
  |             Horror| 5.416894977168945|
  |    Romantic Comedy| 5.572992700729925|
  |             Comedy| 5.506962962962957|
  |       Black Comedy| 6.061111111111112|
  |        Documentary| 6.020930232558141|
  |          Adventure| 5.812408759124091|
  |              Drama| 6.335614702154622|
  |  Thriller/Suspense| 6.201255230125521|
  |             Action| 5.707142857142859|
  +-------------------+------------------+
   */

}
