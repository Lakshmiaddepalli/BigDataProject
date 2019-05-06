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
import com.koddi.geocoder.Geocoder


 //  println(args.mkString(","))

    //val geoWithKey = Geocoder.create("AIzaSyA7iBwz17N_5hySdDSZGSDerO78_b8XUbw")
    // val results = geoWithKey.lookup(args(0))
    // val location = results.head.geometry.location
    // println(s"Latitude: ${location.latitude}, Longitude: ${location.longitude}")

var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimedl1.csv").cache()
//dfcrime.filter(dfcrime("Primary_Type") === "OTHER OFFENSE" || dfcrime("Primary_Type") === "PUBLIC INDECENCY" || dfcrime("Primary_Type") === "DECEPTIVE PRACTICE" || dfcrime("Primary_Type") === "OBSCENITY" || dfcrime("Primary_Type") === "MOTOR VEHICLE THEFT" || dfcrime("Primary_Type") === "CRIMINAL DAMAGE" ||dfcrime("Primary_Type") === "GAMBLING" || dfcrime("Primary_Type") === "LIQUOR LAW VIOLATION" || dfcrime("Primary_Type") === "INTIMIDATION" || dfcrime("Primary_Type") === "NON-CRIMINAL" || dfcrime("Primary_Type") === "CRIMINAL TRESPASS" || dfcrime("Primary_Type") === "PUBLIC PEACE VIOLATION")

val crimerdd = dfcrime.rdd
val crimelocations = crimerdd.map(x => Vectors.dense(x.getDouble(11),x.getDouble(12)))

//var Communityareacount =  dfcrime.groupBy($"Community_Area").count().orderBy(desc("count"))
val crimemodel = KMeans.train(crimelocations, 300, 20)
var crimemapping = crimemodel.predict(crimelocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)

//crimemapping.collect.foreach(println)
var joinval = crimemodel.clusterCenters.zip(crimemapping.collect())
var crimelocationsfin = sc.parallelize(crimemodel.clusterCenters)
var crimepoints = sc.parallelize(joinval)

//crimepoints.collect().foreach(println)
var crimepointsmap = crimepoints.map( c => c._1(0).toString + "," + c._1(1).toString + "," + c._2)
crimepointsmap.coalesce(1).saveAsTextFile("hdfs:///user/sla410/crimedatabigdataproject/crimecluster1.csv")
//crimepointsmap.toDF.coalesce(1).write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/crimecluster.csv")
//Communityareacount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/crimecount.csv")
val toDouble = sqlContext.udf.register("toDouble", ((n: String) => { n.toDouble }))

var dfrestaurant = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/cleanrestaurantsfinal.csv").cache()
var finaldfrestaurant = dfrestaurant.withColumn("Longitude", toDouble(dfrestaurant("Longitude")))
finaldfrestaurant = finaldfrestaurant.withColumn("Latitude", toDouble(finaldfrestaurant("Latitude")))
var restaurantrdd = finaldfrestaurant.rdd
//var restaurantlocations = restaurantrdd.map(x => x(14)+ "," + x(15))
var restaurantlocations = restaurantrdd.map(x => Vectors.dense(x.getDouble(14),x.getDouble(15)))
restaurantlocations.collect().foreach(println)
//var Communityareacount =  dfcrime.groupBy($"Community_Area").count().orderBy(desc("count"))
val restaurantmodel = KMeans.train(restaurantlocations, 300, 20)
var restaurantmapping = restaurantmodel.predict(restaurantlocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)

//crimemapping.collect.foreach(println)
var joinrestaurantval = restaurantmodel.clusterCenters.zip(restaurantmapping.collect())
var restaurantslocationsfin = sc.parallelize(restaurantmodel.clusterCenters)
var restaurantpoints = sc.parallelize(joinrestaurantval)

//crimepoints.collect().foreach(println)
var restaurantpointsmap = restaurantpoints.map( c => c._1(0).toString + "," + c._1(1).toString + "," + c._2)
restaurantpointsmap.coalesce(1).saveAsTextFile("hdfs:///user/sla410/crimedatabigdataproject/restaurantscluster.csv")


var dfsexoffenders = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/SexOffendersfinal.csv").cache()
val sexoffendersrdd = dfsexoffenders.rdd
val sexoffenderslocations = sexoffendersrdd.map(x => Vectors.dense(x.getDouble(10),x.getDouble(11)))
sexoffenderslocations.collect().foreach(println)
val sexoffendersmodel = KMeans.train(sexoffenderslocations, 300, 20)
var sexoffendersmapping = sexoffendersmodel.predict(sexoffenderslocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)
var joinsexoffendersval = sexoffendersmodel.clusterCenters.zip(sexoffendersmapping.collect())
var sexoffenderslocationsfin = sc.parallelize(sexoffendersmodel.clusterCenters)
var sexoffenderspoints = sc.parallelize(joinsexoffendersval)

var sexoffenderspointsmap = sexoffenderspoints.map( c => c._1(0).toString + "," + c._1(1).toString + "," + c._2)
sexoffenderspointsmap.coalesce(1).saveAsTextFile("hdfs:///user/sla410/crimedatabigdataproject/sexoffenderscluster.csv")

//var dfsocioeconomiccensus = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/socioeconomicfactors3.csv").cache()
//var hardshipcount = dfsocioeconomiccensus.select("Community_Area","HARDSHIP_INDEX")
//hardshipcount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/socioeconomiccount.csv")


var dfaffordablehousing = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Affordable_Rental_Housing_Developments.csv").cache()
var affordhouse = dfaffordablehousing.rdd
var affordhouselocations = affordhouse.map(x => Vectors.dense(x.getDouble(11),x.getDouble(12)))
//var affordhouse = dfaffordablehousing.groupBy("Community Area Number").agg(sum("Units"))

val affordinghousesmodel = KMeans.train(affordhouselocations, 300, 20)
var affordinghouseslocationsfin = sc.parallelize(affordinghousesmodel.clusterCenters)
var affordhousemapping = affordinghousesmodel.predict(affordhouselocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)

var joinhouseval = affordinghousesmodel.clusterCenters.zip(affordhousemapping.collect())
var housepoints = sc.parallelize(joinhouseval)

var affordhousemap = housepoints.map( h => h._1(0).toString + "," + h._1(1).toString + "," + h._2)
affordhousemap.coalesce(1).saveAsTextFile("hdfs:///user/sla410/crimedatabigdataproject/affordablehouse.csv")
//affordhouse.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/affordhousecountcommunitywise.csv")

val uniondata = affordinghouseslocationsfin.union(restaurantslocationsfin).cache()
val  uniondata1 = uniondata.union(sexoffenderslocationsfin).cache()
var finalunion = uniondata1.union(crimelocationsfin).cache()
var allmodel = KMeans.train(finalunion, 400, 20)

var housecluster = allmodel.predict(affordinghouseslocationsfin)
var houseclustermap = housecluster.map(r => (r,1))

var foodcluster = allmodel.predict(restaurantslocationsfin)
var foodclustermap = foodcluster.map(r => (r, 2))

var crimecluster = allmodel.predict(crimelocationsfin)
var crimeclustermap = crimecluster.map(r => (r,3))

var sexoffenderscluster = allmodel.predict(sexoffenderslocationsfin)
var sexoffendersclustermap = sexoffenderscluster.map(r => (r,4))

var u1 = foodclustermap.union(crimeclustermap)
var u2 = u1.union(houseclustermap)
var u3 = u2.union(sexoffendersclustermap)

var filterClusterCenters = u3.groupBy(_._1)
//filterClusterCenters.collect().foreach(println)

var filterCluster = filterClusterCenters.map(r => (r._1, r._2.map(_._2).toSet, r._2.map(_._2).size))
//filterCluster.collect().foreach(println)
var removevalues  = filterCluster.filter((r => r._2.contains(3)))
var removevalues1 = filterCluster.filter((r => r._2.contains(4)))
var intermediate = filterCluster.subtract(removevalues)
var modelsfoodandhousing  = intermediate.subtract(removevalues1)
modelsfoodandhousing.collect().foreach(println)

var finalval = modelsfoodandhousing.map(r => (r._1,allmodel.clusterCenters(r._1), r._3))
//finalval.collect().foreach(println)

//2339 N California Ave, Chicago, IL 60647, USA
//var ans = allmodel.predict(Vectors.dense(41.924068,-87.697201))
var ans = allmodel.predict(Vectors.dense(location.latitude.toDouble,location.longitude.toDouble))
var  clusterans  = modelsfoodandhousing.filter(a => a._1 == ans)

var locations = clusterans.map(r => (allmodel.clusterCenters(r._1)))
var locationscordinate = locations.take(1)

var latitude = locationscordinate(0)(0)
var longitude = locationscordinate(0)(1)

var recommendedplace = geoWithKey.lookup(latitude, longitude)
println(recommendedplace)
