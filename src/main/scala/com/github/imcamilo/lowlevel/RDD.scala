package com.github.imcamilo.lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

object RDD extends App {

  val spark = SparkSession
    .builder()
    .appName("Introduction to RDD")
    .config("spark.master", "local")
    .getOrCreate()

  // For Creating a RDD we do need an Spark Context

  // Spark Context is the entry point for creating RDD
  val sc = spark.sparkContext

  // 1 - PARALLELIZE AND EXISTING COLLECTION
  val numbers = 1 to 1000000
  // Distributed collection of Int
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)

  // 2 - A) READING FROM FILES - RDD[StockValue]
  case class StockValue(company: String, date: String, price: Double)
  def readStocks(fileName: String) =
    Source
      .fromFile(fileName)
      .getLines()
      .drop(1)
      .map(l => l.split(","))
      .map(t => StockValue(t(0), t(1), t(2).toDouble))
      .toList

  val stocksRDD: RDD[StockValue] = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2 - B) READING FROM FILES - sc.textFile returns Originally RDD[String]
  val stocks2RDD = sc
    .textFile("src/main/resources/data/stocks.csv")
    .map(l => l.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(t => StockValue(t(0), t(1), t(2).toDouble))

  // 3 - READ FROM A DF
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .format("csv")
    .option("sep", ",") // separator
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  // convert
  import spark.implicits._
  val stocksDS: Dataset[StockValue] = stocksDF.as[StockValue]
  val stocks3RDD = stocksDS.rdd
  // The converstion its necessary because it I dont make the DS first, I will obtain a RDD[Row] and you'll lose the type information
  val stocksXRDD: RDD[Row] = stocksDF.rdd

}
