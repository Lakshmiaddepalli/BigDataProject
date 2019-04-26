spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df4 = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/311_final1.csv")
df4.printSchema()
df4 = df4.withColumnRenamed("LOCATION OF BUILDING ON THE LOT (IF GARAGE, CHANGE TYPE CODE TO BGD).", "LOCATION OF BUILDING ON THE LOT")



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
df4 = df4.withColumn("DateServiceRequest", todate($"DATE SERVICE REQUEST WAS RECEIVED"))
df4 = df4.withColumn("DateServiceRequest", (col("DateServiceRequest").cast("date")))
df4 = df4.drop("DATE SERVICE REQUEST WAS RECEIVED")
df4 = df4.filter($"DateServiceRequest".between("2010-01-01", "2011-12-31"))
val filteredData = df4.select(df4("SERVICE REQUEST TYPE"), date_format(df4("DateServiceRequest"), "yyyy-MM-dd").alias("date")).filter($"date".between("2010-01-01", "2011-12-31"))


df4.filter(df4("SERVICE REQUEST TYPE").isNull || df4("SERVICE REQUEST TYPE") === "" || df4("SERVICE REQUEST TYPE").isNaN).count() //3 filter
df4.filter(df4("SERVICE REQUEST NUMBER").isNull || df4("SERVICE REQUEST NUMBER") === "" || df4("SERVICE REQUEST NUMBER").isNaN).count() //3  //filter
df4.filter(df4("DATE SERVICE REQUEST WAS RECEIVED").isNull || df4("DATE SERVICE REQUEST WAS RECEIVED") === "" || df4("DATE SERVICE REQUEST WAS RECEIVED").isNaN).count() //3 //filter  //format date
df4.filter(df4("LOCATION OF BUILDING ON THE LOT").isNull || df4("LOCATION OF BUILDING ON THE LOT") === "" || df4("LOCATION OF BUILDING ON THE LOT").isNaN).count() //8759  //will drop dont require
df4.filter(df4("IS THE BUILDING DANGEROUS OR HAZARDOUS?").isNull || df4("IS THE BUILDING DANGEROUS OR HAZARDOUS?") === "" || df4("IS THE BUILDING DANGEROUS OR HAZARDOUS?").isNaN).count() //65095 //will drop dont require
df4.filter(df4("IS BUILDING OPEN OR BOARDED?").isNull || df4("IS BUILDING OPEN OR BOARDED?") === "" || df4("IS BUILDING OPEN OR BOARDED?").isNaN).count() //8258  //will drop dont require
df4.filter(df4("IF THE BUILDING IS OPEN, WHERE IS THE ENTRY POINT?").isNull || df4("IF THE BUILDING IS OPEN, WHERE IS THE ENTRY POINT?") === "" || df4("IF THE BUILDING IS OPEN, WHERE IS THE ENTRY POINT?").isNaN).count() //30672 //will drop dont require
df4.filter(df4("IS THE BUILDING VACANT DUE TO FIRE?").isNull || df4("IS THE BUILDING VACANT DUE TO FIRE?") === "" || df4("IS THE BUILDING VACANT DUE TO FIRE?").isNaN).count() //9105 //replace empty values with none
df4.filter(df4("ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)").isNull || df4("ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)") === "" || df4("ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)").isNaN).count() //8545 //replace empty values with none
df4.filter(df4("ADDRESS STREET NUMBER").isNull || df4("ADDRESS STREET NUMBER") === "" || df4("ADDRESS STREET NUMBER").isNaN).count() //4 //Ignore
df4.filter(df4("ADDRESS STREET DIRECTION").isNull || df4("ADDRESS STREET DIRECTION") === "" || df4("ADDRESS STREET DIRECTION").isNaN).count() //5  //Ignore
df4.filter(df4("ADDRESS STREET NAME").isNull || df4("ADDRESS STREET NAME") === "" || df4("ADDRESS STREET NAME").isNaN).count() //4 //Ignore
df4.filter(df4("ADDRESS STREET SUFFIX").isNull || df4("ADDRESS STREET SUFFIX") === "" || df4("ADDRESS STREET SUFFIX").isNaN).count() //325 //Ignore
df4.filter(df4("ZIP CODE").isNull || df4("ZIP CODE") === "" || df4("ZIP CODE").isNaN).count() //428 //drop
df4.filter(df4("X COORDINATE").isNull || df4("X COORDINATE") === "" || df4("X COORDINATE").isNaN).count() //40 //drop
df4.filter(df4("Y COORDINATE").isNull || df4("Y COORDINATE") === "" || df4("Y COORDINATE").isNaN).count() //40 //drop
df4.filter(df4("Ward").isNull || df4("Ward") === "" || df4("Ward").isNaN).count() //44 //drop
df4.filter(df4("Police District").isNull || df4("Police District") === "" || df4("Police District").isNaN).count() //45 drop
df4.filter(df4("Community Area").isNull || df4("Community Area") === "" || df4("Community Area").isNaN).count() //45
df4.filter(df4("LATITUDE").isNull || df4("LATITUDE") === "" || df4("LATITUDE").isNaN).count() //41  //use google api
df4.filter(df4("LONGITUDE").isNull || df4("LONGITUDE") === "" || df4("LONGITUDE").isNaN).count() //41 //use google api
df4.filter(df4("Location").isNull || df4("Location") === "" || df4("Location").isNaN).count() //41 //drop
df4.filter(df4("Historical Wards 2003-2015").isNull || df4("Historical Wards 2003-2015") === "" || df4("Historical Wards 2003-2015").isNaN).count() //158 //drop
df4.filter(df4("Zip Codes").isNull || df4("Zip Codes") === "" || df4("Zip Codes").isNaN).count() //41 //drop
df4.filter(df4("Community Areas").isNull || df4("Community Areas") === "" || df4("Community Areas").isNaN).count() //92 //drop
df4.filter(df4("Census Tracts").isNull || df4("Census Tracts") === "" || df4("Census Tracts").isNaN).count() //drop
df4.filter(df4("Wards").isNull || df4("Wards") === "" || df4("Wards").isNaN).count()  //81 //drop


