package com.github.imcamilo.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypesExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("Complex Types Exercises")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  /*
  1. How do we deal with multiple date formats?
  2. Read the stocks df and parse the dates
   */

  // 1. How do we deal with multiple date formats?
  // parse the df multiple times, then union the small DFs

  // 2. Read the stocks df and parse the dates
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .format("csv")
    .option("sep", ",") // separator
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val dateParsed = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  dateParsed.show()

}
