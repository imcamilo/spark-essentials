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


}
