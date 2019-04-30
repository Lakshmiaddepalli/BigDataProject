spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.mean


val sqlc = new SQLContext(sc)
var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimedl1.csv").cache()
var dfhealth = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/publichealth.csv").cache()
var dfsocioeconomiccensus = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/sla410/crimedatabigdataproject/socioeconomicfactors3.csv").cache()

dfcrime.registerTempTable("crime")
dfhealth.registerTempTable("health")
dfsocioeconomiccensus.registerTempTable("socioeconomiccensus")

var crimefactors = sqlc.sql("""SELECT
        crime.Year,crime.Month,crime.Day,crime.Time,
        crime.IUCR, crime.Primary_Type,crime.Description,crime.Location_Description,
        crime.Community_Area, crime.Arrest,crime.FBI_Code,crime.Latitude,crime.Longitude,
        socioeconomiccensus.PERCENT_OF_HOUSING_CROWDED, 
        socioeconomiccensus.PERCENT_HOUSEHOLDS_BELOW_POVERTY,
        socioeconomiccensus.PERCENT_AGED_16_UNEMPLOYED, 
        socioeconomiccensus.PERCENT_AGED_25_WITHOUT_HIGH_SCHOOL_DIPLOMA,
        socioeconomiccensus.PERCENT_AGED_UNDER_18_OR_OVER_64,
        socioeconomiccensus.PER_CAPITA_INCOME, 
        socioeconomiccensus.HARDSHIP_INDEX,
        health.Birth_Rate,
        health.General_Fertility_Rate,
        health.Low_Birth_Weight,
        health.Prenatal_Care_Beginning_in_First_Trimester,
        health.Preterm_Births,health.Teen_Birth_Rate,health.Assault,
        health.Breast_cancer_in_females,health.Cancer,health.Colorectal_Cancer,
        health.Diabetes_related,health.Firearm_related,
        health.Infant_Mortality_Rate,health.Lung_Cancer,
        health.Prostate_Cancer_in_Males,health.Stroke,
        health.Childhood_Blood_Lead_Level_Screening,
        health.Childhood_Lead_Poisoning,
        health.Gonorrhea_in_Females,
        health.Gonorrhea_in_Males,
        health.Tuberculosis
        FROM crime JOIN socioeconomiccensus 
        ON crime.Community_Area = socioeconomiccensus.Community_Area
        JOIN health
        ON crime.Community_Area = health.Community_Area""".stripMargin)



val toDouble = sqlContext.udf.register("toDouble", ((n: Int) => { n.toDouble }))

val arrestencode = sqlContext.udf.register("arrestencode", (Arrest: Boolean) => {
      if (Arrest)
        1.0
      else
        0.0
    })

crimefactors = crimefactors.withColumn("Arrest", arrestencode(crimefactors("Arrest")))
crimefactors = crimefactors.withColumn("Year", toDouble(crimefactors("Year")))
crimefactors = crimefactors.withColumn("Month", toDouble(crimefactors("Month")))
crimefactors = crimefactors.withColumn("Day", toDouble(crimefactors("Day")))
crimefactors = crimefactors.withColumn("Community_Area", toDouble(crimefactors("Community_Area")))
crimefactors = crimefactors.withColumn("PER_CAPITA_INCOME", toDouble(crimefactors("PER_CAPITA_INCOME")))
crimefactors = crimefactors.withColumn("HARDSHIP_INDEX", toDouble(crimefactors("HARDSHIP_INDEX")))

val timeInd = new StringIndexer().setInputCol("Time").setOutputCol("TimeIndex")
val iucrInd = new StringIndexer().setInputCol("IUCR").setOutputCol("IUCRIndex")
val primarytypeInd = new StringIndexer().setInputCol("Primary_Type").setOutputCol("PrimaryTypeIndex")
val descriptionInd = new StringIndexer().setInputCol("Description").setOutputCol("DescriptionIndex")
val locationdescriptionInd = new StringIndexer().setInputCol("Location_Description").setOutputCol("LocationDescriptionIndex")
val fbicodeInd = new StringIndexer().setInputCol("FBI_Code").setOutputCol("FBICodeIndex")
val gnmalesInd = new StringIndexer().setInputCol("Gonorrhea_in_Males").setOutputCol("GonorrheainMalesIndex")

