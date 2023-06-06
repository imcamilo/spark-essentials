package com.github.imcamilo.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSqlExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/data/warehouse")
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") doesnt work in spark 3
    // The SQL config 'spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation' was removed in the version 3.0.0.
    // It was removed to prevent loss of user data for non-default value.
    .getOrCreate()

  /*
  1. Read movies DF and store it as a table.
  2. Count how many employees were hired between Jan 1 2000 and Jan 1 2001.
  3. Show the average salaries for the employees hired in between those dates, group by department number.
  4. Show the name of the best paying department for employees hired in between those dates.
   */

  // 1
  // create just created_db
  // spark.sql("create database sparksqlpractice")

  // use just created_db
  // spark.sql("use sparksqlpractice")

  // store the table in just created_db
  // val moviesDF = spark.read
  // .option("inferSchema", "true")
  // .json("src/main/resources/data/movies.json")
  // moviesDF.createOrReplaceTempView("movies")
  // moviesDF.write.mode(SaveMode.Overwrite).saveAsTable("movies")

  // 2
  // Count how many employees were hired between Jan 1 2000 and Jan 1 2001.
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

  def transferTables(tableNames: List[String], writeToWH: Boolean = false): Unit = tableNames.foreach { tableName =>
    {
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)
      if (writeToWH)
        tableDF.write
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName) // will save this dataframe under the name employees in the rtjvm im currently using
    }
  }

  transferTables(List("employees", "departments", "dept_manager", "titles", "dept_emp", "salaries"))

  // val empDF = spark.read.table("employees")

  spark
    .sql("""
           |select count(*)
           |from employees
           |where hire_date > '1999-01-01' and hire_date < '2001-01-01'
           |""".stripMargin)
  // .show()

  // 3
  // three way joins
  spark
    .sql(
      """
        |select de.dept_no, avg(s.salary)
        |from employees e, dept_emp de, salaries s
        |where e.hire_date > '1999-01-01' and e.hire_date < '2001-01-01'
        |and e.emp_no = de.emp_no
        |and e.emp_no = s.emp_no
        |group by de.dept_no
        |""".stripMargin
    )
    .show()

  // 4
  // Show the name of the best paying department for employees hired in between those dates.
  spark
    .sql(
      """
        |select avg(s.salary) payments, d.dept_name
        |from employees e, dept_emp de, salaries s, departments d
        |where e.hire_date > '1999-01-01' and e.hire_date < '2001-01-01'
        |and e.emp_no = de.emp_no
        |and e.emp_no = s.emp_no
        |and de.dept_no = d.dept_no
        |group by d.dept_name
        |order by payments desc
        |limit 1
        |""".stripMargin
    )
    .show()

}
