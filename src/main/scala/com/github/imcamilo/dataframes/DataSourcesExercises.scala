package com.github.imcamilo.dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object DataSourcesExercises extends App {

  /** Read the movies DF. Then write it as:
   *    - tab-separated value file.
   *    - snappy Parquet
   *    - table public.movies in postgres
   */
  val spark = SparkSession
    .builder()
    .appName("Data Sources Formats Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesSchema = StructType(
    Array(
      StructField("Title", StringType, nullable = true),
      StructField("US_Gross", LongType, nullable = true),
      StructField("Worldwide_Gross", LongType, nullable = true),
      StructField("US_DVD_Sales", LongType, nullable = true),
      StructField("Production_Budget", LongType, nullable = true),
      StructField("Release_Date", DateType, nullable = true),
      StructField("MPAA_Rating", StringType, nullable = true),
      StructField("Running_Time_min", LongType, nullable = true),
      StructField("Distributor", StringType, nullable = true),
      StructField("Source", StringType, nullable = true),
      StructField("Major_Genre", StringType, nullable = true),
      StructField("Creative_Type", StringType, nullable = true),
      StructField("Director", StringType, nullable = true),
      StructField("Rotten_Tomatoes_Rating", LongType, nullable = true),
      StructField("IMDB_Rating", DoubleType, nullable = true),
      StructField("IMDB_Votes", LongType, nullable = true)
    )
  )

  val moviesDF = spark.read
    //or .read.json("path")
    .schema(moviesSchema)
    .format("json")
    .option("inferSchema", "true")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  moviesDF.printSchema()
  moviesDF.show()

  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    //.csv("src/main/resources/data/movies.csv")
    .save("src/main/resources/data/movies.csv")

  //moviesDF.write.save(path) //default format and default compression is snappy
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("compression", "snappy") // bzip2, gzip, lz4, snappy, deflate
    .parquet("src/main/resources/data/movies.parquet")

  //to DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()

}
