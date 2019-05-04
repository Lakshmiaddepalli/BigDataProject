spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
val sqlc = new SQLContext(sc) 
import sqlc._
import sqlc.implicits._

var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimedl1.csv").cache()
var Communityareacount =  dfcrime.groupBy($"Community_Area").count().orderBy(desc("count"))
Communityareacount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/crimecount.csv")

var dfsexoffenders = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/SexOffendersfinal.csv").cache()
var sexoffenderscount =  dfsexoffenders.groupBy($"Community Area Number").count().orderBy(desc("count"))
sexoffenderscount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/sexoffenderscount.csv")


var dfsocioeconomiccensus = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/socioeconomicfactors3.csv").cache()
var hardshipcount = dfsocioeconomiccensus.select("Community_Area","HARDSHIP_INDEX")
hardshipcount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/socioeconomiccount.csv")


var dfaffordablehousing = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Affordable_Rental_Housing_Developments.csv").cache()
var affordhouse = dfaffordablehousing.groupBy("Community Area Number").agg(sum("Units"))
affordhouse.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/affordhousecountcommunitywise.csv")
