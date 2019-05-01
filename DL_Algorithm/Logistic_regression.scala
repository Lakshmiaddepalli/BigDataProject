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

case class Crime(Year: Double, Month: Double, Day: Double, Time: String, IUCR: String, Primary_Type: String,Description: String, 
                 Location_Description: String,Community_Area: Double, FBI_Code: String, Latitude: Double, Longitude: Double)

object Project {
  
def main (args: Array[String]) {
        
        if (args.length < 12) {
            System.err.println("Please Enter all the inputs. . . .")
            System.exit(1)
        }
        
        
        System.out.println("Starting Spark SQL Context...")
        val sqlc = new SQLContext(sc) 
        import sqlContext._  
        import sqlContext.implicits._ 
        val directory = args(0)

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

val timeInd = new StringIndexer().setInputCol("Time").setOutputCol("TimeIndex").setHandleInvalid("skip")
val iucrInd = new StringIndexer().setInputCol("IUCR").setOutputCol("IUCRIndex").setHandleInvalid("skip")
val primarytypeInd = new StringIndexer().setInputCol("Primary_Type").setOutputCol("PrimaryTypeIndex").setHandleInvalid("skip")
val descriptionInd = new StringIndexer().setInputCol("Description").setOutputCol("DescriptionIndex").setHandleInvalid("skip")
val locationdescriptionInd = new StringIndexer().setInputCol("Location_Description").setOutputCol("LocationDescriptionIndex").setHandleInvalid("skip")
val fbicodeInd = new StringIndexer().setInputCol("FBI_Code").setOutputCol("FBICodeIndex").setHandleInvalid("skip")
val gnmalesInd = new StringIndexer().setInputCol("Gonorrhea_in_Males").setOutputCol("GonorrheainMalesIndex").setHandleInvalid("skip")

//all the correlation values between arrest and other features lied between -0.01 to 0.01 and hence are highly inependent of each other.
valcrime.select(corr($"Arrest",$"Year")).show()

val crimetest = Seq(Crime(args(0).toDouble, args(1).toDouble, args(2).toDouble, args(3),args(4), args(5), args(6), args(7), args(8).toDouble, args(9),args(10).toDouble,args(11).toDouble))
val crimetestdata = sqlc.sparkContext.parallelize(Seq(crimetestdata)).toDF
val joinedhealthdata = health.join(crimetestdata).where('Community_Area === 'Community_Area)
val joinedsocioeconomichealth = socioeconomiccensus.join(joinedhealthdata).where('Community_Area === 'Community_Area)
  
  
val assembler = new VectorAssembler().setInputCols(Array("Year", "Month", "Day", "TimeIndex","IUCRIndex", "PrimaryTypeIndex","DescriptionIndex","LocationDescriptionIndex","Community_Area","FBICodeIndex","Latitude","Longitude","PERCENT_OF_HOUSING_CROWDED","PERCENT_HOUSEHOLDS_BELOW_POVERTY","PERCENT_AGED_16_UNEMPLOYED","PERCENT_AGED_25_WITHOUT_HIGH_SCHOOL_DIPLOMA","PERCENT_AGED_UNDER_18_OR_OVER_64","PER_CAPITA_INCOME","HARDSHIP_INDEX","Birth_Rate","General_Fertility_Rate","Low_Birth_Weight","Prenatal_Care_Beginning_in_First_Trimester","Preterm_Births","Teen_Birth_Rate","Assault","Breast_cancer_in_females","Cancer","Colorectal_Cancer","Diabetes_related","Firearm_related","Infant_Mortality_Rate","Lung_Cancer","Prostate_Cancer_in_Males","Stroke","Childhood_Blood_Lead_Level_Screening","Childhood_Lead_Poisoning","Gonorrhea_in_Females","GonorrheainMalesIndex","Tuberculosis")).setOutputCol("features_temp")
val normalizer = new Normalizer().setInputCol("features_temp").setOutputCol("features").setP(1.0)
val lr = new LogisticRegression().setMaxIter(100000)
lr.setLabelCol("Arrest")

val pipeline = new Pipeline().setStages(Array(timeInd, iucrInd, primarytypeInd,descriptionInd,locationdescriptionInd,fbicodeInd,gnmalesInd, assembler, normalizer,lr))
val splits = valcrime.randomSplit(Array(0.8, 0.2), seed = 11L)
val train = splits(0).cache()
val test = splits(1).cache()

var model = pipeline.fit(train)
var result = model.transform(test)
result = result.select("prediction","Arrest")
val predictionAndLabels = result.map { row =>
 (row.get(0).asInstanceOf[Double],row.get(1).asInstanceOf[Double])
}
val metrics = new BinaryClassificationMetrics(predictionAndLabels)
println("Area under ROC = " + metrics.areaUnderROC()) //Area under ROC = 0.5393021885782492
model = pipeline.fit(train_data)
}
}
