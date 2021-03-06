scp reddit.txt sla410@dumbo.es.its.nyu.edu:/home/sla410/
hdfs dfs -ls hdfs:///user/sla410/
hdfs dfs -mkdir  hdfs:///user/sla410/bigdataproject/
hdfs dfs -put reddit.txt  hdfs:///user/sla410/bigdataproject/
spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df = sqlContext.read.option("header", "true").load("hdfs:///user/sla410/bigdataproject/reddit.txt")

df.collect.foreach(println)
spark-shell --packages com.dataneb.spark

//data cleaning
 def dateclean(date:String) : String = {
	  var ans = date.split("/")
      var yearval = ans(0).toString
      var monthval = ans(1).toString
      var dayval = ans(2).toString
      
      if(monthval.length  < 2){
        monthval = "0" + monthval
        }
        
      if (dayval.length < 2){
        dayval = "0" + dayval 
        }
        
      val dateval = dayval + monthval + yearval
      
      dateval
   }
 
 val todate = udf[String,String](dateclean)
 df = df.withColumn("date", todate($"date"))
 
 
 //data profiling the columns not required
df = df.drop(df.col("title"))


//showing data columns 
df.show(Int.MaxValue)

 //Save the cleanedfile
 df.write.format("com.databricks.spark.csv").save("redditdatasentimentdatemapping.csv")
 var df = sqlContext.read.option("header", "true").load("hdfs:///user/sla410/bigdataproject/redditdatasentimentdatemapping.csv")
 //print the schema
 df.printSchema()
//The ranges and schema has been provided in schema.txt