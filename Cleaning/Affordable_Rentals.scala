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

var dfaffordablehousing = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Affordable_Rental_Housing_Developments.csv").cache()
//dfaffordablehousing.filter(!($"Latitude"===""))

dfaffordablehousing.filter(dfaffordablehousing("Community Area Name").isNull || dfaffordablehousing("Community Area Name") === "" || dfaffordablehousing("Community Area Name").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Community Area Number").isNull || dfaffordablehousing("Community Area Number") === "" || dfaffordablehousing("Community Area Number").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Property Type").isNull || dfaffordablehousing("Property Type") === "" || dfaffordablehousing("Property Type").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Property Name").isNull || dfaffordablehousing("Property Name") === "" || dfaffordablehousing("Property Name").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Address").isNull || dfaffordablehousing("Address") === "" || dfaffordablehousing("Address").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Zip Code").isNull || dfaffordablehousing("Zip Code") === "" || dfaffordablehousing("Zip Code").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Phone Number").isNull || dfaffordablehousing("Phone Number") === "" || dfaffordablehousing("Phone Number").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Management Company").isNull || dfaffordablehousing("Management Company") === "" || dfaffordablehousing("Management Company").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Units").isNull || dfaffordablehousing("Units") === "" || dfaffordablehousing("Units").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("X Coordinate").isNull || dfaffordablehousing("X Coordinate") === "" || dfaffordablehousing("X Coordinate").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Y Coordinate").isNull || dfaffordablehousing("Y Coordinate") === "" || dfaffordablehousing("Y Coordinate").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Latitude").isNull || dfaffordablehousing("Latitude") === "" || dfaffordablehousing("Latitude").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Longitude").isNull || dfaffordablehousing("Longitude") === "" || dfaffordablehousing("Longitude").isNaN).count()
dfaffordablehousing.filter(dfaffordablehousing("Location").isNull || dfaffordablehousing("Location") === "" || dfaffordablehousing("Location").isNaN).count()
