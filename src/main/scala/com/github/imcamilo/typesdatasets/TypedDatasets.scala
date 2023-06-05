package com.github.imcamilo.typesdatasets

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import java.sql.Date

object TypedDatasets extends App {

  val spark = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  // just int prop, this is essentially a distributed collection of numbers

  // CONVERT A DATAFRAME TO A DATASET
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  // now I can handle it like an scala collection.
  // numbersDS.filter(_ > 100)

  // DATASET OF A COMPLEX TYPE, CLASS HAVE TO HAS THE SAME FIELD NAMES AS THE JSON
  // 1. DEFINE A TYPE, CASE CLASS, RARELY YOU WILL NEED ANYTHING ELSE THAN A CASE CLASS
  case class Car(
      Name: String,
      Miles_per_Gallon: Option[Double],
      Cylinders: Long,
      Displacement: Double,
      Horsepower: Option[Long],
      Weight_in_lbs: Long,
      Acceleration: Double,
      Year: String,
      Origin: String
  )
  // 2. READ THE DF
  def readDF(name: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$name")

  // 3. DEFINE AN ENCODER, MOST OF THE TIME IT WILL SOLVE IMPORTING IMPLICITS

  // .product takes a type that extends of Product type, (all the case classes do it)
  // this helps spark identify which fields mapped to each columns in a DF
  // implicit val carEncoder = Encoders.product[Car]
  // maybe write all the encoders could be tedious, thats why spark has its own implicits:
  import spark.implicits._ // import all the encoders that may you want to use
  val carsDF = readDF("cars.json")
  // this works like Circe
  // 4. CONVERT DATA FRAME TO DATA SET
  val carsDS = carsDF.as[Car]


  //DS collection functions
  numbersDS.filter(_ < 100).show()

  //map - flatMap - fold - reduce - for comprehension
  carsDS.map(car => car.Name.toUpperCase()).show()
}
