package com.github.imcamilo.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object GroupingAndAggregationsExercise extends App {

  /*
  1. Sum ALL the profits of ALL the movies in the DF.
  2. Count how many distinct directors we have.
  3. Show the mean and standard deviation of US Gross revenue for the movies.
  4. Compute the average IMDB Rating and the average US Gross revenue PER DIRECTOR
   */

  val spark = SparkSession
    .builder()
    .appName("Aggregations and Grouping Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // 1. SUM ALL THE PROFITS OF ALL THE MOVIES IN THE DF.
  import org.apache.spark.sql.functions.{col, sum}
  val sumUSGrossProfitsOfAllMoviesDF = moviesDF.select(sum("US_Gross"))
  val sumAllGrossInMoviesDF = moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum(col("Total_Gross")))
  // sumAllGrossInMoviesDF.show()
  // total gross: 139190135783

  // 2. COUNT HOW MANY DISTINCT DIRECTORS WE HAVE.
  import org.apache.spark.sql.functions.countDistinct
  val countDistinctDirectors = moviesDF
    .select(countDistinct(col("Director")))
  // countDistinctDirectors.show()
  // TOTAL: 550

  // 3. SHOW THE MEAN AND STANDARD DEVIATION OF US GROSS REVENUE FOR THE MOVIES.
  import org.apache.spark.sql.functions.{mean, stddev}
  val detailsSDDF = moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  )
  // detailsSDDF.show()
  // avg(US_Gross) = 4.4002085163744524E7
  // stddev_samp(US_Gross) = 6.255531139066214E7
  // E7 means times ten to seventh

  // 4. COMPUTE THE AVERAGE IMDB RATING AND THE AVERAGE US GROSS REVENUE PER DIRECTOR
  val avgRatingAndavgGrossRevDF = moviesDF
    .groupBy("Director")
    .agg(
      avg(col("IMDB_Rating")).as("Avg_Rating"),
      avg(col("US_Gross")).as("Avg_US_Gross_Revenue")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
  // trick: desc, desc_nulls_first, desc_nulls_last, asc, asc_nulls_first, asc_nulls_last,
  // avgRatingAndavgGrossRevDF.show()

  // AGGREGATIONS AND GROUPING ARE WIDE TRANSFORMATIONS
  // when one or more input partitions, contributes to one or more output partitions
  // with shuffles, that means the data is being moved between diff nodes in spark cluster
  // shuffling its an expensive operation.

  // IF YOU HAVE PIPELINES THAT YOU ALSO WANT TO BE REALLY FAST, BE CAREFUL WHEN YOU ARE USING DATA AGGREGATIONS AND
  // GROUPING.
  // OFTEN IN THE CASE, NOT ALWAYS BUT OFTEN ITS THE CASE THAT IT WOULD BE BEST TO DO DATA AGGREGATIONS AND GROUPING
  // AT THE END OF THE PROCESSING

}
