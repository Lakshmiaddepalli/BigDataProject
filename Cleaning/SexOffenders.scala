//SexOffenders Cleaning

spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df3 = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/SexOffendersfinal.csv")
df3.printSchema()

df3.filter(df3("LAST").isNull || df3("LAST") === "" || df3("LAST").isNaN).count() //gave 0
df3.filter(df3("FIRST").isNull || df3("FIRST") === "" || df3("FIRST").isNaN).count() //gave 0
df3.filter(df3("BLOCK").isNull || df3("BLOCK") === "" || df3("BLOCK").isNaN).count() //gave 0
df3.filter(df3("GENDER").isNull || df3("GENDER") === "" || df3("GENDER").isNaN).count() //gave 0
df3.filter(df3("RACE").isNull || df3("RACE") === "" || df3("RACE").isNaN).count() //gave 0
df3.filter(df3("BIRTH DATE").isNull || df3("BIRTH DATE") === "" || df3("BIRTH DATE").isNaN).count() //gave 0
df3.filter(df3("AGE").isNull || df3("AGE") === "" || df3("AGE").isNaN).count() //gave 0
df3.filter(df3("HEIGHT").isNull || df3("HEIGHT") === "" || df3("HEIGHT").isNaN).count() //gave 0
df3.filter(df3("WEIGHT").isNull || df3("WEIGHT") === "" || df3("WEIGHT").isNaN).count() //gave 0
df3.filter(df3("VICTIM MINOR").isNull || df3("VICTIM MINOR") === "" || df3("VICTIM MINOR").isNaN).count() //gave 0
df3.filter(df3("latitude").isNull || df3("latitude") === "" || df3("latitude").isNaN).count() //gave 0
df3.filter(df3("longitude").isNull || df3("longitude") === "" || df3("longitude").isNaN).count() //gave 0
df3.filter(df3("Community Area Name").isNull || df3("Community Area Name") === "" || df3("Community Area Name").isNaN).count() //gave 0
df3.filter(df3("Community Area Number").isNull || df3("Community Area Number") === "" || df3("Community Area Number").isNaN).count() //gave 0


//This Dataset is cleaned