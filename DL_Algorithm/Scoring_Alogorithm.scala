
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
val sqlc = new SQLContext(sc) 
import sqlContext._
import sqlContext.implicits._

var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimedl1.csv").cache()
var Communityareacount =  dfcrime.groupBy($"Community_Area").count().orderBy(desc("count"))
Communityareacount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/crimecount.csv")

var dfsexoffenders = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/SexOffendersfinal.csv").cache()
var sexoffenderscount =  dfsexoffenders.groupBy($"Community Area Number").count().orderBy(desc("count"))
sexoffenderscount.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/sexoffenderscount.csv")
