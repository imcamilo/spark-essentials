package com.github.imcamilo.typesdatasets

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

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

  // DS collection functions
  numbersDS.filter(_ < 100).show()

  // map - flatMap - fold - reduce - for comprehension
  // carsDS.map(car => car.Name.toUpperCase()).show()

  // JOINS

  case class Guitar(id: Long, model: String, make: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: List[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandDS = readDF("bands.json").as[Band]

  // if you use join you will lose the type information, use joinWith
  // now I have a dataset of tuples
  val guitarPlayerBandDS: Dataset[(GuitarPlayer, Band)] =
    guitarPlayersDS.joinWith(bandDS, guitarPlayersDS.col("band") === bandDS.col("id"), "inner")

  /*
  +--------------------+--------------------+
  |                  _1|                  _2| You can rename the columns calling .withColumnRenamed
  +--------------------+--------------------+
  |{1, [1], 1, Angus...|{Sydney, 1, AC/DC...|
  |{0, [0], 0, Jimmy...|{London, 0, Led Z...|
  |{3, [3], 3, Kirk ...|{Los Angeles, 3, ...|
  +--------------------+--------------------+

  this is the only main difference between joining Dataframes and joining Datasets, obtaining a datasets of tuples.
   */

  guitarPlayerBandDS.show

}
