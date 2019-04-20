﻿spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
from pyspark.sql.functions import col


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/dj1322/crime_project/311_Service_Requests_-_Vacant_and_Abandoned_Buildings_Reported_-_Historical.csv")
df = df.withColumn("Date_of_Service", $"DATE SERVICE REQUEST WAS RECEIVED")




 def dateclean(date:String) : String = {
        var ans = date.split("/")
      var yearval = ans(2).toString
      var monthval = ans(0).toString
      var dayval = ans(1).toString
      
      if(monthval.length  < 2){
        monthval = "0" + monthval
        }
        
      if (dayval.length < 2){
        dayval = "0" + dayval 
        }
      if (yearval.length < 4){
        yearval = "20" + dayval 
        }
      val dateval = dayval + "/" + monthval + "/" + yearval
      
      dateval
   }


// val todate = udf[String,String](dateclean)
// df = df.withColumn("Date_of_Service", todate($"Date_of_Service"))
// df = df.filter($"Date_of_Service".between("01/01/2008", "01/01/2012"))




df.printSchema()
df.filter(df("SERVICE REQUEST TYPE").isNull || df("SERVICE REQUEST TYPE") === "" || df("SERVICE REQUEST TYPE").isNaN).count() //gave 3


df = df.filter(!($"SERVICE REQUEST TYPE"===""))


df.filter(df("SERVICE REQUEST NUMBER").isNull || df("SERVICE REQUEST NUMBER") === "" || df("SERVICE REQUEST NUMBER").isNaN).count() // gave 0






df.filter(df("IS THE BUILDING CURRENTLY VACANT OR OCCUPIED?").isNull || df("IS THE BUILDING CURRENTLY VACANT OR OCCUPIED?") === "" || df("IS THE BUILDING CURRENTLY VACANT OR OCCUPIED?").isNaN).count() // 8248


df = df.filter(!($"IS THE BUILDING CURRENTLY VACANT OR OCCUPIED?"===""))




df.filter(df("ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)").isNull || df("ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)") === "" || df("ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)").isNaN).count() //305


df = df.filter(!($"ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)"===""))


df.filter(df("IS THE BUILDING VACANT DUE TO FIRE?").isNull || df("IS THE BUILDING VACANT DUE TO FIRE?") === "" || df("IS THE BUILDING VACANT DUE TO FIRE?").isNaN).count() //613


df = df.filter(!($"IS THE BUILDING VACANT DUE TO FIRE?"===""))






Todo: Gather block mapping and fill longitude and latitude then we don’t need to filter out these
//we filter out data that doesn’t have X Coordinate as it is an important parameter as 
df = df.filter(!($"X COORDINATE"===""))


//we filter out data that doesn’t have Y Coordinate as it is an important parameter
df = df.filter(!($"Y COORDINATE"===""))


//we filter out data that doesn’t have Latitude as it is an important parameter
df = df.filter(!($"LATITUDE"===""))


//we filter out data that doesn’t have Longitude as it is an important parameter
df = df.filter(!($"LONGITUDE"===""))


Todo get its latitude and longitude and get the community area
//we filter out data that doesn’t Community Area as it is an important parameter 
df.filter(df("Community Area").isNull || df("Community Area") === "" || df("Community Area").isNaN).count()


df = df.filter(!($"Community Area"===""))