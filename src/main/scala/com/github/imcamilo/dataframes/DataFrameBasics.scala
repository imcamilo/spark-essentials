package com.github.imcamilo.dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFrameBasics extends App {

  // 1. CREATING SPARK SESSION
  val spark = SparkSession
    .builder()
    .appName("DataFrame Basics")
    .config("spark.master", "local") // executing in this computer
    .getOrCreate()

  // spark session is our entry point for reading and writing data frames


  // 2. READING DATA FRAME
  // structure of the data is applied to the data frame in the form of a schema, which is the description
  // of the fields known as columns and the types for all those columns
  val firstDF: DataFrame = spark.read
    .format("json") // json file
    // its much the same that option, infering the schema all the columns will figured out from the json
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  println("showing details")
  // printing the df, only showing top 20 rows
  firstDF.show()
  // schema
  firstDF.printSchema()

  // we can think the DataFrame as a distributed collection.
  firstDF.take(20).foreach(println) // it will take the first 20

  // SPARK TYPES
  println("spark types")
  // these types are used by spark, (case objects) used internally
  // to describe the type of the data inside a data frame
  val longType = LongType

  // schema of a potential data frame
  // this is how can create a schema, which is often useful in practice
  // in practice you will use inferSchema
  val customCarsSchema = StructType(
    Array( // describe columns
      StructField("Acceleration", DoubleType, nullable = true),
      StructField("Cylinders", LongType, nullable = true), // int type
      StructField("Displacement", DoubleType, nullable = true), // int type
      StructField("Horsepower", LongType, nullable = true), // int type
      StructField("Miles_per_Gallon", DoubleType, nullable = true), // int type
      StructField("Name", StringType, nullable = true),
      StructField("Origin", StringType, nullable = true),
      StructField("Weight_in_lbs", LongType, nullable = true), // int type
      StructField("Year", StringType, nullable = true)
    )
  )

  // this its the same
  val carsDFSchema: StructType = firstDF.schema
  println("cars struct type:")
  println(carsDFSchema)
  // in prod you should define your own schema, because now, the date is loaded as string with this format: 1970-01-01
  // READ DF with your OWN schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  //Rows would be a unstructured piece of data
  val myRow = Row("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA")

  // test: create rows by hand
  // val myRow = carsListRow.head
  // create row by tuples
  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15, 8, 350, 165, 3693, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18, 8, 318, 150, 3436, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16, 8, 304, 150, 3433, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17, 8, 302, 140, 3449, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15, 8, 429, 198, 4341, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14, 8, 454, 220, 4354, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14, 8, 440, 215, 4312, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14, 8, 455, 225, 4425, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15, 8, 390, 190, 3850, 8.5, "1970-01-01", "USA")
  )
  // the types of tuples will known at compile time
  // schema auto inferred
  val manualCarsDF = spark.createDataFrame(cars)
  println("manual cars df created")
  // NOTE: DF HAS SCHEMAS, ROWS DO NOT
  // schema its only applicable to dataframes not to rows

  // tricks

  // create a DF with implicits

  import spark.implicits._

  val manualCarsDFWithImplicits = cars.toDF(
    "Name",
    "MPG",
    "Cylinders",
    "Displacement",
    "HP",
    "Weight",
    "Acceleration",
    "Year",
    "Origin"
  )

  //
  println("schemas ... and implicits")
  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()
}
