package com.github.imcamilo.lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

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
  case class StockValue(symbol: String, date: String, price: Double)
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

  // CONVERT FROM RDD TO DF/DS

  // RDD -> DataFrame
  // if we have an rdd with multiple fields/columns, must pass the column names as well
  // you lose the type information, doesnt have any types, does have rows
  val numbersDF: DataFrame = numbersRDD.toDF("numbers")

  // RDD -> DataSet
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD) // Dataset[Int]

  // TRANSFORMATIONS
  // Once we have an RDD we can process it as any other collection
  // considering our case class StockValue(symbol: String, date: String, price: Double)
  // stocksRDD, stocks2RDD, stocks3RDD
  // Lazy transformations are not apply until spark needs to
  val microsoftRDD: RDD[StockValue] = stocksRDD.filter(s => s.symbol == "MSFT") // Lazy Transformation
  val msCount = microsoftRDD.count() // Eager Action
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  implicit val orderLessThan1: Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  // implicit val orderLessThan2 = Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  // implicit val orderLessThan3 = Ordering.fromLessThan((sa: StockValue, sb: StockValue) => sa.price < sb.price)

  // No implicit arguments of type: Ordering[StockValue]
  // min() is an Action because its reducing all the RDD to a single value
  val minMsft = microsoftRDD.min() // (orderLessThan1) just a implicit

  // REDUCE
  numbersRDD.reduce(_ + _) // just a sum of numbers

  // GROPING
  val groupedStocksRDD: RDD[(String, Iterable[StockValue])] = stocksRDD.groupBy(_.symbol) // group by company name
  // grouping is very expensive.

  // PARTITIONING
  // RDD has the capability of choosing how they are going to be partitioned
  // example: stocksRDD to 30 partitions
  val repartitionedStocksRDD: RDD[StockValue] = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stocks30")
  // this generates the folder with 30 files

  /*
   REPARTITIONING IS EXPENSIVE, BY DEFINITIONS IT INVOLVE MOVING DATA IN BETWEEN SPARK NODES, INVOLVE SHUFFLING

   THE BEST PRACTICE FOR REPARTITIONING IS TO DO THE REPARTITION EARLY IN YOUR SPARK WORKFLOW.
   SO: PARTITION EARLY THEN PROCESS THAT.

   HOW MANY PARTITIONS SHOULD I HAVE?
   SIZE OF PARTITION SHOULD BE 10 AND A 100 MB (10MB-100MB).
   */

  // COALESCE
  // it takes 2 arguments, number of partitions and shuffling as boolean.
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // Doesn't involve Shuffling
  coalescedRDD.toDF.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stocks15")
  // this generates the folder with 15 files

  /*
     COALESCE WILL REPARTITION OUR RDD TO FEWER PARTITIONS THAT IT CURRENTLY HAS.
     Coalesce will not necessary involve shuffling, the data doesn't need to be moved in between the entire cluster.
     In this case , at least 15 partitions are going to stay in the same place, and the other partitions are going
     to move the data to them.

     This is not a true shuffling in between the cluster.
     */
}
