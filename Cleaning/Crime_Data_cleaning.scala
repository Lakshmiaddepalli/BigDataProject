spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Crimes_-_2001_to_present.csv")
df = df.withColumn("Date_of_Crime", split($"Date", " ")(0))
df = df.withColumn("Time", split($"Date", " ")(1))
df = df.drop("Date")

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
df = df.withColumn("Date_of_Crime", (col("Date_of_Crime").cast("date")))
df = df.filter($"Date_of_Crime".between("2008-01-01", "2011-12-31"))


df.printSchema()
df.filter(df("ID").isNull || df("ID") === "" || df("ID").isNaN).count() //gave 0
df.filter(df("Case Number").isNull || df("Case Number") === "" || df("Case Number").isNaN).count() //gave 0 
df.filter(df("Block").isNull || df("Block") === "" || df("Block").isNaN).count() //0
df.filter(df("IUCR").isNull || df("Block") === "" || df("IUCR").isNaN).count() //0
df.filter(df("Primary Type").isNull || df("Primary Type") === "" || df("Primary Type").isNaN).count() //0
df.filter(df("Description").isNull || df("Description") === "" || df("Description").isNaN).count() //0
df.filter(df("Location Description").isNull || df("Location Description") === "" || df("Location Description").isNaN).count() //17
df.filter(df("Arrest").isNull || df("Arrest") === "" || df("Arrest").isNaN).count() //0
df.filter(df("Beat").isNull || df("Beat") === "" || df("Beat").isNaN).count() //0
df.filter(df("District").isNull || df("District") === "" || df("District").isNaN).count() //47
df.filter(df("Ward").isNull || df("Ward") === "" || df("Ward").isNaN).count() 
df.filter(df("Community Area").isNull || df("Community Area") === "" || df("Community Area").isNaN).count() //8
df.filter(df("FBI Code").isNull || df("FBI Code") === "" || df("FBI Code").isNaN).count() //0
df.filter(df("X Coordinate").isNull || df("X Coordinate") === "" || df("X Coordinate").isNaN).count() //850
df.filter(df("Y Coordinate").isNull || df("Y Coordinate") === "" || df("Y Coordinate").isNaN).count() //850
df.filter(df("Year").isNull || df("Year") === "" || df("Year").isNaN).count()   //0
df.filter(df("Updated On").isNull || df("Updated On") === "" || df("Updated On").isNaN).count() //0
df.filter(df("Latitude").isNull || df("Latitude") === "" || df("Latitude").isNaN).count() //850
df.filter(df("Longitude").isNull || df("Longitude") === "" || df("Longitude").isNaN).count() //850
df.filter(df("Location").isNull || df("Location") === "" || df("Location").isNaN).count() //850
df.filter(df("Date_of_Crime").isNull || df("Date_of_Crime") === "" || df("Date_of_Crime").isNaN).count() //0
df.filter(df("Time").isNull || df("Time") === "" || df("Time").isNaN).count() //0


//Todo: Gather block mapping and fil longitude and latitude then we dont need to filter out these
//we filter out data that doesnt have X Coordinate as it is an important parameter as 
df = df.filter(!($"X Coordinate"===""))

//we filter out data that doesnt have Y Coordinate as it is an important parameter
df = df.filter(!($"Y Coordinate"===""))

//we filter out data that doesnt have Latitude as it is an important parameter
df = df.filter(!($"Latitude"===""))

//we filter out data that doesnt have Longitude as it is an important parameter
df = df.filter(!($"Longitude"===""))


//we filter out data that doesnt have location description as it is an important parameter
df.filter(!($"Location Description"===""))

//Todo get its latitude and longitude and get the community area
//we filter out data that doesnt Community Area as it is an important parameter 
df = df.filter(!($"Community Area"===""))