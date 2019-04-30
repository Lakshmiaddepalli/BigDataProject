val sqlc = new SQLContext(sc)
var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimedl1.csv")
var dfhealth = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/publichealth.csv")
var dfsocioeconomiccensus = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/socioeconomicfactors2.csv")
