import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.koddi.geocoder.Geocoder
import org.apache.spark.rdd.RDD


object SafePlacesToLive {

  def main(args: Array[String]): Unit = {
    println("Starting")
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SafePlacesToLive")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlc = new SQLContext(sc)
    import sqlc._
    import sqlc.implicits._

    println(args.mkString(","))

    val geoWithKey = Geocoder.create("AIzaSyA7iBwz17N_5hySdDSZGSDerO78_b8XUbw")
    val results = geoWithKey.lookup(args(0))
    val location = results.head.geometry.location
    println(s"Latitude: ${location.latitude}, Longitude: ${location.longitude}")
    var gender = args(1)
    println(gender)

    var genderval = false;

    def isFemale(stringval : String) = {
      if(args(1) == "Female"){
         genderval = true;
      }
      genderval
    }

    var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/crimedlfinal.csv").cache()
    //dfcrime.printSchema()

    dfcrime.filter(dfcrime("Primary_Type") === "WEAPONS VIOLATION")
    val crimerdd = dfcrime.rdd
    val crimelocations = crimerdd.map(x => Vectors.dense(x.getDouble(11),x.getDouble(12)))
    val crimemodel = KMeans.train(crimelocations, 300, 20)
    var crimemapping = crimemodel.predict(crimelocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)
    //crimemapping.collect.foreach(println)
    var joinval = crimemodel.clusterCenters.zip(crimemapping.collect())
    var crimelocationsfin = sc.parallelize(crimemodel.clusterCenters)
    var crimepoints = sc.parallelize(joinval)
    //scrimepoints.collect().foreach(println)
    var crimepointsmap = crimepoints.map( c => c._1(0).toString + "," + c._1(1).toString + "," + c._2)
   // crimepointsmap.coalesce(1).saveAsTextFile("src/main/resources/crimecluster.csv")

    println("crime clusters created..........")



    val toDouble = sqlc.udf.register("toDouble", ((n: String) => { n.toDouble }))




    var dfrestaurant = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/restaurants.csv").cache()
    var finaldfrestaurant = dfrestaurant.withColumn("Longitude", toDouble(dfrestaurant("Longitude")))
    finaldfrestaurant = finaldfrestaurant.withColumn("Latitude", toDouble(finaldfrestaurant("Latitude")))
    var restaurantrdd = finaldfrestaurant.rdd
    //var restaurantlocations = restaurantrdd.map(x => x(14)+ "," + x(15))
    var restaurantlocations = restaurantrdd.map(x => Vectors.dense(x.getDouble(14),x.getDouble(15)))
    //restaurantlocations.collect().foreach(println)
    val restaurantmodel = KMeans.train(restaurantlocations, 300, 20)
    var restaurantmapping = restaurantmodel.predict(restaurantlocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)
    var joinrestaurantval = restaurantmodel.clusterCenters.zip(restaurantmapping.collect())
    var restaurantslocationsfin = sc.parallelize(restaurantmodel.clusterCenters)
    var restaurantpoints = sc.parallelize(joinrestaurantval)
    var restaurantpointsmap = restaurantpoints.map( c => c._1(0).toString + "," + c._1(1).toString + "," + c._2)
    //restaurantpointsmap.coalesce(1).saveAsTextFile("src/main/resources/restaurantscluster.csv")

    println("restaurant clusters created..........")

    var dfaffordablehousing = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/Affordable_Rental_Housing_Developments.csv").cache()
    var affordhouse = dfaffordablehousing.rdd
    var affordhouselocations = affordhouse.map(x => Vectors.dense(x.getDouble(11),x.getDouble(12)))
    //var affordhouse = dfaffordablehousing.groupBy("Community Area Number").agg(sum("Units"))
    val affordinghousesmodel = KMeans.train(affordhouselocations, 300, 20)
    var affordhousemapping = affordinghousesmodel.predict(affordhouselocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)
    var joinhouseval = affordinghousesmodel.clusterCenters.zip(affordhousemapping.collect())
    var affordinghouseslocationsfin = sc.parallelize(affordinghousesmodel.clusterCenters)
    var housepoints = sc.parallelize(joinhouseval)
    var affordhousemap = housepoints.map( h => h._1(0).toString + "," + h._1(1).toString + "," + h._2)
    //affordhousemap.coalesce(1).saveAsTextFile("src/main/resources/affordablehousecluster.csv")

    println("affordable housing clusters created..........")


      var dfsexoffenders = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/SexOffendersfinal.csv").cache()
      val sexoffendersrdd = dfsexoffenders.rdd
      val sexoffenderslocations = sexoffendersrdd.map(x => Vectors.dense(x.getDouble(10),x.getDouble(11)))
      //sexoffenderslocations.collect().foreach(println)
      val sexoffendersmodel = KMeans.train(sexoffenderslocations, 300, 20)
      var sexoffendersmapping = sexoffendersmodel.predict(sexoffenderslocations).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._2)
      var joinsexoffendersval = sexoffendersmodel.clusterCenters.zip(sexoffendersmapping.collect())
      var sexoffenderslocationsfin = sc.parallelize(sexoffendersmodel.clusterCenters)
      var sexoffenderspoints = sc.parallelize(joinsexoffendersval)

      var sexoffenderspointsmap = sexoffenderspoints.map( c => c._1(0).toString + "," + c._1(1).toString + "," + c._2)
//      sexoffenderspointsmap.coalesce(1).saveAsTextFile("src/main/resources/sexoffenderscluster.csv")


    val uniondata = affordinghouseslocationsfin.union(restaurantslocationsfin).cache()
    var finalunion = uniondata.union(crimelocationsfin).cache()
    if(genderval){
      finalunion = finalunion.union(sexoffenderslocationsfin).cache()

    }

    var allmodel = KMeans.train(finalunion, 400, 20)

    println("clusters created.................")

    var housecluster = allmodel.predict(affordinghouseslocationsfin)
    var houseclustermap = housecluster.map(r => (r,1))

    var foodcluster = allmodel.predict(restaurantslocationsfin)
    var foodclustermap = foodcluster.map(r => (r, 2))

    var crimecluster = allmodel.predict(crimelocationsfin)
    var crimeclustermap = crimecluster.map(r => (r,3))

    var sexoffenderscluster = allmodel.predict(crimelocationsfin)
    var sexoffendersclustermap = crimecluster.map(r => (r,3))

    if(genderval){
      sexoffenderscluster = allmodel.predict(sexoffenderslocationsfin)
      sexoffendersclustermap = sexoffenderscluster.map(r => (r,4))
    }

    var u1 = foodclustermap.union(crimeclustermap)
    var u2 = u1.union(houseclustermap)

    if(genderval){
      u2 = u2.union(sexoffendersclustermap)
    }

    var filterClusterCenters = u2.groupBy(_._1)
    //filterClusterCenters.collect().foreach(println)

    var filterCluster = filterClusterCenters.map(r => (r._1, r._2.map(_._2).toSet, r._2.map(_._2).size))
    //filterCluster.collect().foreach(println)
    var removevalues  = filterCluster.filter((r => r._2.contains(3)))
    var modelsfoodandhousing  = filterCluster.subtract(removevalues)

    if(genderval){
      var removevalues1  = filterCluster.filter((r => r._2.contains(4)))
      modelsfoodandhousing = modelsfoodandhousing.subtract(removevalues1)
    }

  //  println(modelsfoodandhousing.count())
  //  modelsfoodandhousing.collect().foreach(println)

    var safeclusters = modelsfoodandhousing.map( h => h._1.toString + "," + h._2(0).toString + "," + h._2(1).toString)
 //   safeclusters.collect().foreach(println)
//    safeclusters.coalesce(1).saveAsTextFile("src/main/resources/safeclusters.csv")
    var finalval = modelsfoodandhousing.map(r => (r._1,allmodel.clusterCenters(r._1), r._3))
   // finalval.collect().foreach(println)

    //2339 N California Ave, Chicago, IL 60647, USA
    //var ans = allmodel.predict(Vectors.dense(41.924068,-87.697201))

    def isEmpty[T](rdd : RDD[T]) = {
      rdd.take(1).length == 0
    }

    var ans = allmodel.predict(Vectors.dense(location.latitude.toDouble,location.longitude.toDouble))
   // println(ans)
    var  clusterans  = modelsfoodandhousing.filter(a => a._1 == ans)
   // println(clusterans.collect().foreach(println))

    if(!isEmpty(clusterans)) {
      var locations = clusterans.map(r => (allmodel.clusterCenters(r._1)))
     // println(locations.collect().foreach(println))
      var locationscordinate = locations.take(1)
     //s println(locations.collect().foreach(println))

      var latitude = locationscordinate(0)(0)
      var longitude = locationscordinate(0)(1)

      var recommendedplace = geoWithKey.lookup(latitude, longitude)
      println("The Nearby Recommended Safe Places are ..............")
      //println(recommendedplace.head)
      //println(recommendedplace(0))
      recommendedplace.foreach((element) => println(element+" "))

    }
    else{
      println("No Nearby Recommended Safe Places..............")
    }
  }
}
