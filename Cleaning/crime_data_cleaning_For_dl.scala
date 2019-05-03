spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat

val sqlContext = new org.apache.spark.sql.SQLContext(sc)


spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimefinal.csv")
df = df.withColumn("Date_of_Crime", split($"Date", " ")(0))

def dateclean(date:String) : String = {
	  var ans = date.split("/")
      var yearval = ans(2).toString
      var monthval = ans(0).toString
      var dayval = ans(1).toString
      
      if(monthval.length()  < 2){
        monthval = "0" + monthval
        }
        
      if (dayval.length() < 2){
        dayval = "0" + dayval 
        }
      if (yearval.length() < 4){
        yearval = "20" + dayval 
        }
      val dateval = yearval + "-" + monthval  + "-" + dayval
      dateval
   }

val todate = udf[String,String](dateclean)
df = df.withColumn("Date_of_Crime", todate($"Date_of_Crime"))
df =  df.withColumnRenamed("Year", "Year_of_crime")
df = df.withColumn("Year", split($"Date_of_Crime", "-")(0))
df = df.withColumn("Day", split($"Date_of_Crime", "-")(2))
df = df.withColumn("Month", split($"Date_of_Crime", "-")(1))
df = df.withColumn("Date_of_Crime", (col("Date_of_Crime").cast("date")))
df = df.filter($"Date_of_Crime".between("2010-01-01", "2011-12-31"))


def timeclean(date:String) : String = {
	  var finalans = date 
	  var ans = date.split(" ")
	  
	  if(ans.length == 3){
	  var input = date;
	  var inputFormat = new SimpleDateFormat("MM/dd/yyyy KK:mm:ss a");
      var outputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
      finalans = outputFormat.format(inputFormat.parse(input));
	  }
	  var timeval = finalans.split(" ")(1)
      timeval
   }
   
val totime = udf[String,String](timeclean)
df = df.withColumn("Time", totime($"Date"))


df  = df.withColumnRenamed("Primary Type", "Primary_Type")
df  = df.withColumnRenamed("Location Description", "Location_Description")
df  = df.withColumnRenamed("Community Area", "Community_Area")
df  = df.withColumnRenamed("FBI Code", "FBI_Code")

var finaldf = df.select("Year","Month","Day","Time","IUCR","Primary_Type","Description","Location_Description","Community_Area","Arrest","FBI_Code","Latitude","Longitude")

finaldf.filter(finaldf("Year").isNull || finaldf("Year") === "" || finaldf("Year").isNaN).count() 
finaldf.filter(finaldf("Month").isNull || finaldf("Month") === "" || finaldf("Month").isNaN).count() 
finaldf.filter(finaldf("Day").isNull || finaldf("Day") === "" || finaldf("Day").isNaN).count() 
finaldf.filter(finaldf("Time").isNull || finaldf("Time") === "" || finaldf("Time").isNaN).count() 
finaldf.filter(finaldf("Primary_Type").isNull || finaldf("Primary_Type") === "" || finaldf("Primary_Type").isNaN).count() 
finaldf.filter(finaldf("Description").isNull || finaldf("Description") === "" || finaldf("Description").isNaN).count() 
finaldf.filter(finaldf("Location_Description").isNull || finaldf("Location_Description") === "" || finaldf("Location_Description").isNaN).count() 
finaldf.filter(finaldf("Community_Area").isNull || finaldf("Community_Area") === "" || finaldf("Community_Area").isNaN).count() 
finaldf.filter(finaldf("Arrest").isNull || finaldf("Arrest") === "" || finaldf("Arrest").isNaN).count() 
finaldf.filter(finaldf("FBI_Code").isNull || finaldf("FBI_Code") === "" || finaldf("FBI_Code").isNaN).count() 
finaldf.filter(finaldf("Latitude").isNull || finaldf("Latitude") === "" || finaldf("Latitude").isNaN).count() 
finaldf.filter(finaldf("Longitude").isNull || finaldf("Longitude") === "" || finaldf("Longitude").isNaN).count() 


1. //we ignore out data that doesnt have X Coordinate as it is an important parameter as 
2. //we ignore out data that doesnt have Y Coordinate as it is an important parameter
3. //Gather block mapping and used google maps to get the longitude and latitude  we need
4. //we filter out data that doesnt have Longitude and latitude as it is an important parameter
5.//we filter out data that doesnt have location description as it is an important parameter
finaldf = finaldf.filter(!($"Location_Description"===""))
6.//we get its latitude and longitude and get the community area using square euclidian distance


finaldf.write.format("csv").option("header", "true").save("hdfs:///user/sla410/crimedatabigdataproject/crimedl.csv")
