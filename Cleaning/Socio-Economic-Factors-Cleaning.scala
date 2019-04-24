spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df2 = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Census_Data_-_Selected_socioeconomic_indicators_in_Chicago__2008___2012.csv")
df2 = df2.withColumnRenamed("Community Area Number", "Community Area")
df2.printSchema()
df2.filter(df2("Community Area").isNull || df2("Community Area") === "" || df2("Community Area").isNaN).count() //1 (we will not delete it as community area the last row represents full Chicago Economic factors)
df2.filter(df2("COMMUNITY AREA NAME").isNull || df2("COMMUNITY AREA NAME") === "" || df2("COMMUNITY AREA NAME").isNaN).count() 
df2.filter(df2("PERCENT OF HOUSING CROWDED").isNull || df2("PERCENT OF HOUSING CROWDED") === "" || df2("PERCENT OF HOUSING CROWDED").isNaN).count() 
df2.filter(df2("PERCENT HOUSEHOLDS BELOW POVERTY").isNull || df2("PERCENT HOUSEHOLDS BELOW POVERTY") === "" || df2("PERCENT HOUSEHOLDS BELOW POVERTY").isNaN).count() 
df2.filter(df2("PERCENT AGED 16+ UNEMPLOYED").isNull || df2("PERCENT AGED 16+ UNEMPLOYED") === "" || df2("PERCENT AGED 16+ UNEMPLOYED").isNaN).count() 
df2.filter(df2("PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA").isNull || df2("PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA") === "" || df2("PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA").isNaN).count() 
df2.filter(df2("PERCENT AGED UNDER 18 OR OVER 64").isNull || df2("PERCENT AGED UNDER 18 OR OVER 64") === "" || df2("PERCENT AGED UNDER 18 OR OVER 64").isNaN).count() 
df2.filter(df2("PER CAPITA INCOME ").isNull || df2("PER CAPITA INCOME ") === "" || df2("PER CAPITA INCOME ").isNaN).count() //0
df2.filter(df2("HARDSHIP INDEX").isNull || df2("HARDSHIP INDEX") === "" || df2("HARDSHIP INDEX").isNaN).count() // 1  (we will not delete it as community area the last row represents full Chicago Economic factors)
