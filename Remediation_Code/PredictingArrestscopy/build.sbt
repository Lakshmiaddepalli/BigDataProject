name := "PredictingArrests"

version := "0.1"

scalaVersion := "2.11.2"


val sparkVersion = "1.6.0"
val sqlVersion = "1.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0"
)
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "com.koddi" %% "geocoder" % "1.1.0"