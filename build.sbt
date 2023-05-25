import Dependencies._

ThisBuild / scalaVersion     := "2.13.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.github.imcamilo"
ThisBuild / organizationName := "camilo"


val sparkVersion = "3.3.2"
val postgresVersion = "42.5.4"

lazy val root = (project in file("."))
  .settings(
    name := "spark-review",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.postgresql" % "postgresql" % postgresVersion
      // Add any other Spark dependencies you need
    ),
    libraryDependencies += munit % Test
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
