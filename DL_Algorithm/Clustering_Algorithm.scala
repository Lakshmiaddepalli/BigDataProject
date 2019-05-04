spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

val sqlc = new SQLContext(sc) 
import sqlc._
import sqlc.implicits._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import scala.collection.mutable.ArrayBuffer


var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimedl1.csv").cache()
val crimerdd = dfcrime.rdd
val crimelocations = crimerdd.map(x => Vectors.dense(x.getDouble(11),x.getDouble(12)))

//var Communityareacount =  dfcrime.groupBy($"Community_Area").count().orderBy(desc("count"))
val crimemodel = KMeans.train(crimelocations, 100, 20)
var crimemapping = crimemodel.predict(crimelocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)

//crimemapping.collect.foreach(println)
var joinval = crimeModel.clusterCenters.zip(crimemapping.collect())
var crimepoints = sc.parallelize(joinval)

//crimepoints.collect().foreach(println)
var crimepointsmap = crimepoints.map( c => c._1(0).toString + "," + c._1(1).toString + "," + c._2)
crimepointsmap.coalesce(1).saveAsTextFile("hdfs:///user/sla410/crimedatabigdataproject/crimecluster1.csv")
//crimepointsmap.toDF.coalesce(1).write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/crimecluster.csv")
//Communityareacount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/crimecount.csv")

val sqlc = new SQLContext(sc) 
import sqlc._
import sqlc.implicits._

var dfrestaurant = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/cleanrestaurants.csv").cache()
val restaurantrdd = dfrestaurant.rdd
val restaurantlocations = restaurantrdd.map(x => Vectors.dense(x.getDouble(15),x.getDouble(16)))

//var Communityareacount =  dfcrime.groupBy($"Community_Area").count().orderBy(desc("count"))
val restaurantmodel = KMeans.train(restaurantlocations, 100, 20)
var restaurantmapping = restaurantmodel.predict(restaurantlocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)

//crimemapping.collect.foreach(println)
var joinrestaurantval = restaurantmodel.clusterCenters.zip(restaurantmapping.collect())
var restaurantpoints = sc.parallelize(joinrestaurantval)

//crimepoints.collect().foreach(println)
var restaurantpointsmap = restaurantpoints.map( c => c._1(0).toString + "," + c._1(1).toString + "," + c._2)
restaurantpointsmap.coalesce(1).saveAsTextFile("hdfs:///user/sla410/crimedatabigdataproject/restaurantscluster.csv")


var dfsexoffenders = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/SexOffendersfinal.csv").cache()
var sexoffenderscount =  dfsexoffenders.groupBy($"Community Area Number").count().orderBy(desc("count"))
sexoffenderscount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/sexoffenderscount.csv")


//var dfsocioeconomiccensus = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/socioeconomicfactors3.csv").cache()
//var hardshipcount = dfsocioeconomiccensus.select("Community_Area","HARDSHIP_INDEX")
//hardshipcount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/socioeconomiccount.csv")


var dfaffordablehousing = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Affordable_Rental_Housing_Developments.csv").cache()
var affordhouse = dfaffordablehousing.rdd
var affordhouselocations = affordhouse.map(x => Vectors.dense(x.getDouble(11),x.getDouble(12)))
//var affordhouse = dfaffordablehousing.groupBy("Community Area Number").agg(sum("Units"))

val affordinghousesmodel = KMeans.train(affordhouselocations, 50, 20)
var affordhousemapping = affordinghousesmodel.predict(affordhouselocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)

var joinhouseval = affordinghousesmodel.clusterCenters.zip(affordhousemapping.collect())
var housepoints = sc.parallelize(joinhouseval)

var affordhousemap = housepoints.map( h => h._1(0).toString + "," + h._1(1).toString + "," + h._2)
affordhousemap.coalesce(1).saveAsTextFile("hdfs:///user/sla410/crimedatabigdataproject/affordablehouse.csv")
//affordhouse.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/affordhousecountcommunitywise.csv")

