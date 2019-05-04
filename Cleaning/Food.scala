spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans

val sqlc = new SQLContext(sc) 
import sqlc._
import sqlc.implicits._

var dfRestaurant = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Food_Inspections_modified.csv").cache()
var types = dfRestaurant.groupBy($"Risk").count()
types.collect().foreach(println)

var cleanrestaurants = dfRestaurant.filter(dfRestaurant("Risk") === "Risk 3 (Low)")
cleanrestaurants = cleanrestaurants.filter(!($"Latitude").isNull)
cleanrestaurants = cleanrestaurants.filter(!($"Longitude").isNull)

cleanrestaurants.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/cleanrestaurantsfinal.csv")



