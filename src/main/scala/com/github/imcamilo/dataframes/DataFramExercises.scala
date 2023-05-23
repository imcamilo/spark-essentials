package com.github.imcamilo.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFramExercises extends App {

  /** Exercise: 1) Create a manual DF describing smartphones
   *    - make
   *    - model
   *    - screen dimmension
   *    - camera megapixels
   *
   *  2) Read another file from a data folder, e.g. movies.json
   *    - count number of rows.
   */

  // 1
  val spark: SparkSession = SparkSession
    .builder()
    .appName("DataFrame Exercise 1")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val smartphones = Seq(
    ("Samsung", "Galaxy", "Android", 12),
    ("Apple", "iPhone X", "iOS", 13),
    ("Nokia", "3310", "The Best", 0) // todo check nulls later
  )

  val smartphonesDF = smartphones.toDF("Make", "Model", "Platform", "CameraMegapixels")
  smartphonesDF.show() // check dataframe

  val manualDFSmartphones = StructType(
    Array(
      StructField("make", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("platform", StringType, nullable = true),
      StructField("camera_megapixels", IntegerType, nullable = true)
    )
  )

  // 2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true") //In production you wont use inferSchema true, You'll use a custom schema
    .load("src/main/resources/data/movies.json")

  println("counting")
  moviesDF.show()
  moviesDF.printSchema()
  println(s"The Movies DF has ${moviesDF.count()} rows")

}
