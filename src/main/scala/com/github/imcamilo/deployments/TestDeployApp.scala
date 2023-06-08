package com.github.imcamilo.deployments

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object TestDeployApp {

  def main(args: Array[String]): Unit = {

    /** movies.json as arg(0) good_comedies.json as arg(1) good comedy = genre == Comedy and IMDB_Rating > 6.5
     */
    if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }
    println(s"ok running. parameters: ${args.mkString(", ")}")
    val spark = SparkSession
      .builder()
      .appName("Test Deploy App")
      .getOrCreate()

    val moviesDF = spark.read.option("inferSchema", "true").json(args(0))

    val goodComediesDF = moviesDF
      .select(
        col("Title"),
        col("IMDB_Rating").as("Rating"),
        col("Release_Date").as("Release")
      )
      .where(
        col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5
      )
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))

  }

}
