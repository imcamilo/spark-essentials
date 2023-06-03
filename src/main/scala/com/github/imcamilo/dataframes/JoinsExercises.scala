package com.github.imcamilo.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

object JoinsExercises extends App {

  /*
    1. Show all employees and their max salary (employees).
    2. Show all employees who were never managers (not matched in dept_manager).
    3. Find the job titles of the best paid 10 employees in the company (titles).
   */

  val spark = SparkSession
    .builder()
    .appName("Joins Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  def readTable(name: String) =
    spark.read
      .format("jdbc")
      .options(
        Map(
          "driver" -> "org.postgresql.Driver",
          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user" -> "docker",
          "password" -> "docker",
          "dbtable" -> name
        )
      )
      .load()

  val employeesDF = readTable("public.employees")
  val salariesDF = readTable("public.salaries")
  val deptManagerDF = readTable("public.dept_manager")
  val titlesDF = readTable("public.titles")

  // employeesDF.show()

  // 1. SHOW ALL EMPLOYEES AND THEIR MAX SALARY (EMPLOYEES)

  val maxSalariesPerEmployeeNumberDF = salariesDF
    .groupBy("emp_no")
    .agg(
      max("salary").as("maxSalary")
    )
  val employeesSalariesDF = employeesDF
    .join(maxSalariesPerEmployeeNumberDF, "emp_no")
  // employeesSalariesDF.show()

  // 2. SHOW ALL EMPLOYEES WHO WERE NEVER MANAGERS (NOT MATCHED IN DEPT_MANAGER).

  val empNeverManagersDF = employeesDF
    .join(deptManagerDF, employeesDF.col("emp_no") === deptManagerDF.col("emp_no"), "left_anti")
  // empNeverManagersDF.show()

  // 3. FIND THE JOB TITLES OF THE BEST PAID 10 EMPLOYEES IN THE COMPANY(TITLES).
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")
  bestPaidJobsDF.show()

}
