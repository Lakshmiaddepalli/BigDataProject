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


var valcrime = crimefactors.na.drop()
val toDouble = sqlContext.udf.register("toDouble", ((n: Int) => { n.toDouble }))

val arrestencode = sqlContext.udf.register("arrestencode", (Arrest: Boolean) => {
      if (Arrest)
        1.0
      else
        0.0
    })

valcrime = valcrime.withColumn("Arrest", arrestencode(valcrime("Arrest")))
valcrime = valcrime.withColumn("Year", toDouble(valcrime("Year")))
valcrime = valcrime.withColumn("Month", toDouble(valcrime("Month")))
valcrime = valcrime.withColumn("Day", toDouble(valcrime("Day")))
valcrime = valcrime.withColumn("Community_Area", toDouble(valcrime("Community_Area")))
valcrime = valcrime.withColumn("PER_CAPITA_INCOME", toDouble(valcrime("PER_CAPITA_INCOME")))
valcrime = valcrime.withColumn("HARDSHIP_INDEX", toDouble(valcrime("HARDSHIP_INDEX")))

val timeInd = new StringIndexer().setInputCol("Time").setOutputCol("TimeIndex")
val iucrInd = new StringIndexer().setInputCol("IUCR").setOutputCol("IUCRIndex")
val primarytypeInd = new StringIndexer().setInputCol("Primary_Type").setOutputCol("PrimaryTypeIndex")
val descriptionInd = new StringIndexer().setInputCol("Description").setOutputCol("DescriptionIndex")
val locationdescriptionInd = new StringIndexer().setInputCol("Location_Description").setOutputCol("LocationDescriptionIndex")
val fbicodeInd = new StringIndexer().setInputCol("FBI_Code").setOutputCol("FBICodeIndex")
val gnmalesInd = new StringIndexer().setInputCol("Gonorrhea_in_Males").setOutputCol("GonorrheainMalesIndex")

val assembler = new VectorAssembler().setInputCols(Array("Year", "Month", "Day", "TimeIndex","IUCRIndex", "PrimaryTypeIndex","DescriptionIndex","LocationDescriptionIndex","Community_Area","FBICodeIndex","Latitude","Longitude","PERCENT_OF_HOUSING_CROWDED","PERCENT_HOUSEHOLDS_BELOW_POVERTY","PERCENT_AGED_16_UNEMPLOYED","PERCENT_AGED_25_WITHOUT_HIGH_SCHOOL_DIPLOMA","PERCENT_AGED_UNDER_18_OR_OVER_64","PER_CAPITA_INCOME","HARDSHIP_INDEX","Birth_Rate","General_Fertility_Rate","Low_Birth_Weight","Prenatal_Care_Beginning_in_First_Trimester","Preterm_Births","Teen_Birth_Rate","Assault","Breast_cancer_in_females","Cancer","Colorectal_Cancer","Diabetes_related","Firearm_related","Infant_Mortality_Rate","Lung_Cancer","Prostate_Cancer_in_Males","Stroke","Childhood_Blood_Lead_Level_Screening","Childhood_Lead_Poisoning","Gonorrhea_in_Females","GonorrheainMalesIndex","Tuberculosis")).setOutputCol("features_temp")
val normalizer = new Normalizer().setInputCol("features_temp").setOutputCol("features").setP(1.0)
val lr = new LogisticRegression().setMaxIter(10)
lr.setLabelCol("Arrest")

val pipeline = new Pipeline().setStages(Array(timeInd, iucrInd, primarytypeInd,descriptionInd,locationdescriptionInd,fbicodeInd,gnmalesInd, assembler, normalizer,lr))
val splits = valcrime.randomSplit(Array(0.8, 0.2), seed = 11L)
val train = splits(0).cache()
val test = splits(1).cache()
