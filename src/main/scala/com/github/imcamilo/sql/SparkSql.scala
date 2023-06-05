package com.github.imcamilo.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSql extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/data/warehouse")
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") doesnt work in spark 3
    // The SQL config 'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation' was removed in the version 3.0.0.
    // It was removed to prevent loss of user data for non-default value.
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DataFrame API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // How to select all the american cars in SQL
  // creates an alias
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF: DataFrame = spark.sql("""
                                              |select Name from cars where origin = 'USA'
                                              |""".stripMargin)
  // americanCarsDF.show()

  // for example, this would return a new dataframe
  // we can run any sql statement
  spark.sql("create database rtjvm")
  // sparks creates spark-warehouse folder, where its store all our databases
  // change the spark warehouse path in spark session configs.
  // .config("spark.sql.warehouse.dir", "src/main/resources/data/warehouse")
  // then we can use it
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
  // databasesDF.show()

  // How to transfer tables from database to spark tables?

  def readTable(name: String) =
    spark.read
      .format("jdbc")
      .options(
        Map(
          "driver" -> "org.postgresql.Driver",
          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user" -> "docker",
          "password" -> "docker",
          "dbtable" -> s"public.$name"
        )
      )
      .load()

  def transferTables(tableNames: List[String]): Unit = tableNames.foreach { tname =>
    {
      val tableDF = readTable(tname)
      tableDF.createOrReplaceTempView(tname)
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tname) // will save this dataframe under the name employees in the rtjvm im currently using
    }
  }

  // Can not create the managed table('`employees`') already exists... it will fail even tho .mode(SaveMode.Overwrite)
  // in spark 2.4 the default override mechanism has changed
  // to fix it, add the config. ("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
  // transferTables(List("employees", "departments", "dept_manager", "titles", "dept_emp", "salaries"))

  // That's how you can tranfer tables from a regular database into spark database, into a data warehouse.

  // Read DataFrame from warehouse:
  // val employeesDF2: DataFrame = spark.read.table("dept_emp")
  // this will be loaded as DataFrame

}
