package com.github.imcamilo.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypesExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // filter the carsDF by a list of car names obtained by an API call

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")
  val complexRegex = getCarNames.map(_.toLowerCase).mkString("|")

  carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
    )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")
    //.show()

  val carNamesFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter =
    carNamesFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()
}
