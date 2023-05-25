package com.github.imcamilo.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExpressionsAndColumns extends App {

  val spark = SparkSession
    .builder()
    .appName("DF Expression and Columns")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
  // carsDF.show()

  // COLUMNS
  // .col takes an arg which is the column name that we want to obtain. this has no data inside.
  // the result of this call, this column object will be used in select expressions
  val firstColumn = carsDF.col("Name")

  // obtaining a new DataFrame out of the original DF.
  // selecting (projecting)
  val carsNameDF = carsDF.select(firstColumn)
  // carsNameDF.show()

  // various select methods
  // fancy way to use columns:
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"), // same with diff, joins
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto converted to column
    $"Horsepower", // fancier interpolated string, returns a column object
    expr("Origin") // Expression
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  // column is a subtype of expression.
  val simplestExpression = carsDF.col("Weight_in_lbs")
  // we want to transform our weight from lbs to kgs.
  // this returns a Column, every value will be divided by 2.2
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2
  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kgs"),
    // also you can use an expression
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs_2")
  )
  // carsWithWeightDF.show()

  // SELECT EXPR
  // like a shortcut for passing many expressions as you want
  val carsWithSelectExprWeight = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )
  // carsWithSelectExprWeight.show()

  // DF Processing
  // 1. ADDING A COLUMN
  // withColumn will extend the original DF with an additional column.
  // passing the name and one expression
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kgs_3", col("Weight_in_lbs") / 2.2)
  // carsWithKg3DF.show()

  // 2. Rename a Column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // carsWithColumnRenamed.show()
  // carefull with column names, if them have spaces can break the compiled expression.
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // .show()

  // 3. REMOVE A COLUMN
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // 4. FILTERING
  // Use =!= for not equals
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  // europeanCarsDF.show()
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // europeanCarsDF2.show()
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA' ")
  val americanCarsDF2 = carsDF.where(col("Origin") === "USA")

  // 5. CHAINING
  val americanPowerfullCarsDF = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)
  val americanPowerfullCars2DF = carsDF
    .filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfullCars3DF = carsDF
    .filter("Origin = 'USA' and Horsepower > 150")

  // 6. UNIONING (ADDING MORE ROWS)
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  // append these 2 cars to the cars df
  // works if DFs have the same schema
  val allCarsDF = carsDF.union(moreCarsDF)

  //7. DISTINCT
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()
}
