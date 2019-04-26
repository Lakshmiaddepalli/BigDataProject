scp finalbitcoin.csv sga297@dumbo.es.its.nyu.edu:/home/sga297/
hdfs dfs -ls hdfs:///user/sga297/
hdfs dfs -put finalbitcoin.csv  hdfs:///user/sga297/bigdataproject/
spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/sga297/bigdataproject/finalbitcoin.csv")
df.collect.foreach(println)

//data cleaning
 def dateclean(date:String) : String = {
	  var ans = date.split("-")
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
 df.collect.foreach(println)

//reordered the columns 
val columns: Array[String] = df.columns
val reorderedColumnNames: Array[String] = Array("date","high","close","low")
df = df.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
 
//data profiling the columns not required
df = df.drop(df.col("mcap"))
df = df.drop(df.col("open"))
df = df.drop(df.col("volume"))
  
//showing data columns 
df.show(Int.MaxValue)

 //Save the cleanedfile
 df.write.format("com.databricks.spark.csv").save("cleaneddata.csv")
 
 //print the schema
 df.printSchema()
//The ranges and schema has been provided in schema.txt

