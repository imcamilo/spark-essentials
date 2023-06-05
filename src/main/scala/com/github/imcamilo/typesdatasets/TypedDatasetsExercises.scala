package com.github.imcamilo.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TypedDatasetsExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

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
  import spark.implicits._ // import all the encoders that may you want to use
  val carsDF = readDF("cars.json")
  // 4. CONVERT DATA FRAME TO DATA SET
  val carsDS = carsDF.as[Car]

  // map - flatMap - fold - reduce - for comprehension
  // carsDS.map(car => car.Name.toUpperCase()).show()

  /*
  1. Count how many cars we have
  2. Count how many POWERFUL cars we have (HP > 140)
  3. Average HP for the entire dataset
   */

  // 1 cars: 406
  val totalCars = carsDS.count()
  println(s"cars: $totalCars")

  // 2 powerful cars: 81
  val pwCars = carsDS.filter(c => c.Horsepower.nonEmpty).filter(c => c.Horsepower.get > 140).count()
  // val pwCars2 = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()
  println(
    s"powerful cars: $pwCars"
  )

  // 3 |       105.0825|
  val avgCarsPower = carsDS.map(c => c.Horsepower.getOrElse(0L)).reduce(_ + _) / totalCars
  println(avgCarsPower)

  carsDS.select(avg("Horsepower")).show()
  carsDS
    .agg(
      avg("Horsepower")
    )
    .show()
}
