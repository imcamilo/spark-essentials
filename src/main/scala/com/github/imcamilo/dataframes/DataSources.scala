package com.github.imcamilo.dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {

  val spark = SparkSession
    .builder()
    .appName("Data Sources Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType, nullable = true),
      StructField("Miles_per_Gallon", DoubleType, nullable = true),
      StructField("Cylinders", LongType, nullable = true),
      StructField("Displacement", DoubleType, nullable = true),
      StructField("Horsepower", LongType, nullable = true),
      StructField("Weight_in_lbs", LongType, nullable = true),
      StructField("Acceleration", DoubleType, nullable = true),
      StructField("Year", StringType, nullable = true),
      StructField("Origin", StringType, nullable = true)
    )
  )

  /*
  Reading a DF:
  - json
  - schema or inferSchema as true
  - zero or more options
    - mode: what to do if there is malformed records
   */
  val carsDF = spark.read
    .format("json")
    // .option("inferSchema", "true")
    .schema(carsSchema) // enforcement schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    // .option("path", "src/main/resources/data/cars.json").load() //file in my pc, s3, etc.
    .load("src/main/resources/data/cars.json")

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(
      Map(
        "inferSchema" -> "true",
        "mode" -> "failFast",
        "path" -> "src/main/resources/data/cars.json"
      )
    )
    .load()

  // action
  // carsDF.show()
  // carsDFWithOptionMap.show()

  /*
   Writing a DF:
    - format
    - save mode = overwrite, append, ignore, errorIfExist
    - path
    - zero or more options

    it generates a folder that contains a bunch of files.
    _SUCCESS file which is just a marker file for spark for validate the completion of the
    writing phase of the data.
    and .crc files for validating the integrity of the other files.
   */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    // .mode("overwrite")
    // .option("path", "src/main/resources/data/cars_dupe.json").save()
    .save("src/main/resources/data/cars_dupe.json")

}
