package com.github.imcamilo.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession
    .builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  // 15-Jan-99
  val moviesWithReleaseDates = moviesDF
    .select(col("title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // returns today
    .withColumn("Right_Now", current_timestamp()) // returns the second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // day differences
  // .show()
  // date_add, date_sub

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()

  // Structs

  // combine "US_Gross" "Worldwide_Gross" into a single column, stored like an array

  // 1 - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
  // .show()

  // 2 - with expression strings, with dot notation
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF.select(
    col("Title"),
    split(
      col("Title"),
      " |,"
    ).as(
      "Title_Words"
    ) // spilts takes the values inside column title separating it with rgx and returns an array of strings
  )

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()

}
