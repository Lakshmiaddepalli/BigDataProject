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

var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimedl1.csv").cache()
var dfcrimefinal = dfcrime.filter(dfcrime("Primary_Type") === "OTHER OFFENSE" || dfcrime("Primary_Type") === "PUBLIC INDECENCY" || dfcrime("Primary_Type") === "DECEPTIVE PRACTICE" || dfcrime("Primary_Type") === "OBSCENITY" || dfcrime("Primary_Type") === "MOTOR VEHICLE THEFT" || dfcrime("Primary_Type") === "CRIMINAL DAMAGE" ||dfcrime("Primary_Type") === "GAMBLING" || dfcrime("Primary_Type") === "LIQUOR LAW VIOLATION" || dfcrime("Primary_Type") === "INTIMIDATION" || dfcrime("Primary_Type") === "NON-CRIMINAL" || dfcrime("Primary_Type") === "CRIMINAL TRESPASS" || dfcrime("Primary_Type") === "PUBLIC PEACE VIOLATION")
var crimerdd = dfcrimefinal.rdd
var crimelocations = crimerdd.map(x => Vectors.dense(x.getDouble(11),x.getDouble(12)))
//var Communityareacount =  dfcrime.groupBy($"Community_Area").count().orderBy(desc("count"))
//Communityareacount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/crimecount.csv")

var dfsexoffenders = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/SexOffendersfinal.csv").cache()
var sexoffenderscount =  dfsexoffenders.groupBy($"Community Area Number").count().orderBy(desc("count"))
sexoffenderscount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/sexoffenderscount.csv")


var dfsocioeconomiccensus = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/socioeconomicfactors3.csv").cache()
var hardshipcount = dfsocioeconomiccensus.select("Community_Area","HARDSHIP_INDEX")
hardshipcount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/socioeconomiccount.csv")


var dfaffordablehousing = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Affordable_Rental_Housing_Developments.csv").cache()
var affordhouse = dfaffordablehousing.groupBy("Community Area Number").agg(sum("Units"))
affordhouse.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/affordhousecountcommunitywise.csv")
