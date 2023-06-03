package com.github.imcamilo.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {

  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // JOIN
  // join receives:
  // first param: the data frame and the
  // second param: join expression (it will be the condition by which spark wil compare the rows)
  // third optional para: join type, default value is "inner":
  //        INNER, will take all the rows from the first DF, all the rows from the second DF,
  //        and will compare them based on the expression, discarding non matched.

  // Inner Join
  val joinCondition = guitaristDF.col("band") === bandDF.col("id") // good practice extract condition
  val guitaristBandsDF = guitaristDF.join(bandDF, joinCondition, "inner")
  // guitaristBandsDF.show()

  // Outer Join

  // left outer
  // contains everything in the innerJoin + all the rows in the LEFT table, with nulls when the data is missing.
  val guitaristBandsLeftOuterDF = guitaristDF.join(bandDF, joinCondition, "left_outer")
  // guitaristBandsLeftOuterDF.show()

  // right outer
  // contains everything in the innerJoin + all the rows in the RIGHT table, with nulls when the data is missing.
  val guitaristBandsRightOuterDF = guitaristDF.join(bandDF, joinCondition, "right_outer")
  // guitaristBandsRightOuterDF.show()

  // full outer join
  // contains everything in the innerJoin + all the rows in BOTH tables, with nulls when the data is missing.
  val guitaristBandsOuterDF = guitaristDF.join(bandDF, joinCondition, "outer")
  // guitaristBandsOuterDF.show()

  // semi joins
  // I will only see the rows in the left data frame for which there is a row in the right DF satisfying the condition
  val guitaristBandsLeftSemiDF = guitaristDF.join(bandDF, joinCondition, "left_semi")
  // guitaristBandsLeftSemiDF.show()

  // anti-join
  // I will only see the rows in the left data frame for which there is no rows in the right DF satisfying the condition
  val guitaristBandsAntiJoinDF = guitaristDF.join(bandDF, joinCondition, "left_anti")
  guitaristBandsAntiJoinDF.show()

  // Things to bear in mind
  // this will crash, spark will not know which column am I referring with this identifier id
  // Reference 'id' is ambiguous, could be: id, id.
  // guitaristBandsDF.select("id", "band").show()
  // how to solve this:
  // option 1 - rename the column on which we are joining
  guitaristDF.join(bandDF.withColumnRenamed("id", "band"), "band")
  // option 2 - drop the dupe column
  guitaristBandsDF.drop(bandDF.col("id"))
  // option 3 - rename the offending column and keep the data
  val bandModifiedDF = bandDF.withColumnRenamed("id", "bandId")
  guitaristDF.join(bandModifiedDF, guitaristDF.col("band") === bandModifiedDF.col("bandId"))

  // using complex types
  guitaristDF.join(
    guitarDF
      .withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)")
  )

}
