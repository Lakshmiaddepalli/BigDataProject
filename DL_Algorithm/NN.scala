//https://www.kdnuggets.com/2015/04/deep-learning-fight-crime.html
//https://blog.cloudera.com/blog/2015/10/how-to-build-a-machine-learning-app-using-sparkling-water-and-apache-spark/
module load spark/2.1.0
spark-shell --packages ai.h2o:sparkling-water-core_2.11:2.1.12,ai.h2o:sparkling-water-ml_2.11:2.1.12,ai.h2o:h2o-genmodel:3.10.0.7,org.apache.spark:spark-mllib_2.11:2.2.0,com.databricks:spark-csv_2.10:1.5.0,no.priv.garshol.duke:duke:1.2

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.{DataFrame, SQLContext}
import java.net._
import org.apache.spark._
import _root_.hex.deeplearning.DeepLearningModel
import _root_.hex.genmodel.utils._
import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import _root_.hex.{Model}
import water.fvec.{Chunk, NewChunk, Vec}
import water.parser.{BufferedString, ParseSetup}
import water.support.{H2OFrameSupport, ModelMetricsSupport, SparkContextSupport, SparklingWaterApp}



object Crime {
  def apply(Year:Short, Month:Byte, Day:Byte,Time: Byte,
            IUCR: Short,Primary_Type: String,Description: String, 
            Location_Description: String,Community_Area: Byte, FBI_Code: Byte,
            Latitude: Double,Longitude: Double): Crime = {
            Crime(Year,Month,Day,Time,
            IUCR, Primary_Type, Description,Location_Description,
            Community_Area, FBI_Code,Latitude, Longitude)
  }
}

case class Crime(Year: Short, Month: Byte, Day: Byte, Time: Byte, IUCR: Short,
                 Primary_Type: String,Description: String, Location_Description: String,
                 Community_Area: Byte, FBI_Code: Byte, Latitude: Double, Longitude: Double)

object predictcrime {


    def main(args:Array[String]): Unit = {
      
      /*if (args.length < 3) {
            System.err.println("Usage: Project <path to directory>")
            System.exit(1)
        }*/
        
        System.out.println("Starting Spark Context...")
        
         //   val sc = new SparkContext()
        val hc = H2OContext.getOrCreate(sc)
        val sqlc = new SQLContext(sc)
        
        import hc._
        import hc.implicits._
        import sqlc.implicits._

        //val crime = asDataFrame(crimedata("hdfs:///user/sla410/crimedatabigdataproject/crimedl.csv"))
        var dfcrime = sqlc.read.format("com.databricks.spark.csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/crimedl1.csv")
        
        // val health = asDataFrame(healthdata("hdfs:///user/sla410/crimedatabigdataproject/publichealth.csv"))
        var dfhealth = sqlc.read.format("com.databricks.spark.csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/publichealth.csv")
       
        // var socioeconomiccensus = asDataFrame(socioeconomiccensusdata("socioeconomicfactors1.csv"))
        var dfsocioeconomiccensus = sqlc.read.format("com.databricks.spark.csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/socioeconomicfactors3.csv")
    
        dfcrime.registerTempTable("crime")
        dfhealth.registerTempTable("health")
        dfsocioeconomiccensus.registerTempTable("socioeconomiccensus")

      //  sqlc.sql("SELECT * FROM crime").show(5)
     //   sqlc.sql("SELECT * FROM health").show(5)
     //   sqlc.sql("SELECT * FROM socioeconomic").show(5)
        
        val crimefactors = sqlc.sql("""SELECT
        crime.Year,crime.Month,crime.Day,crime.Time,
        crime.IUCR, crime.Primary_Type,crime.Description,crime.Location_Description,
        crime.Community_Area, crime.Arrest,crime.FBI_Code,crime.Latitude,crime.Longitude,
        socioeconomiccensus.PERCENT_OF_HOUSING_CROWDED, 
        socioeconomiccensus.PERCENT_HOUSEHOLDS_BELOW_POVERTY,
        socioeconomiccensus.PERCENT_AGED_16+__UNEMPLOYED, 
        socioeconomiccensus.PERCENT_AGED_25+__WITHOUT_HIGH_SCHOOL_DIPLOMA,
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

        val crimeFactorsdataframe:H2OFrame = crimefactors
        H2OFrameSupport.allStringVecToCategorical(crimeFactorsdataframe)
        
        
               def nnModel(train: H2OFrame, test: H2OFrame, colval: String) (implicit hc: H2OContext) : DeepLearningModel = {
       import hc.implicits._
       import _root_.hex.deeplearning.DeepLearning
       val dlval = new DeepLearningParameters()
       dlval._train = train
       dlval._valid = test
       dlval._response_column = colval
       dlval._epochs = 100
       dlval._l1 = 0.0001
       dlval._l2 = 0.0001
       dlval._activation = Activation.RectifierWithDropout
       dlval._hidden = Array(10,10)
       val nn = new DeepLearning(dlval)
       val nnmodel = nn.trainModel.get
       nnmodel
        }

      
      def scorevalue(crime: Crime, socioeconomiccensusdata: DataFrame, healthdata:DataFrame, nnmodel: Model[_,_,_])
      (implicit sqlc: SQLContext, hc: H2OContext): Float = {
      import hc.implicits._
      import sqlc.implicits._
      val crimetestdata = sqlc.sparkContext.parallelize(Seq(crime)).toDF
      val joinedhealthdata = healthdata.join(crimetestdata).where('Community_Area === 'Community_Area)
      val joinedsocioeconomichealth = socioeconomiccensusdata.join(joinedhealthdata).where('Community_Area === 'Community_Area)
      H2OFrameSupport.allStringVecToCategorical(joinedsocioeconomichealth)
      val chanceofarrest = nnmodel.score(joinedsocioeconomichealth).vec("true").at(0)
      chanceofarrest.toFloat 
  
    }

        
        val keyfiles = Array[String]("train.hex", "test.hex")
        val splitratio = Array[Double](0.8)
        val files = H2OFrameSupport.split(crimeFactorsdataframe, keyfiles, splitratio)
        val (trainset, testset) = (files(0), files(1))
        val nnModeldl = nnModel(trainset, testset, 'Arrest)(hc)
       // val (trainMetricvalues, testMetricsvalues) = binomialMetrics(nnModel, trainset, testset)

       // println(s"""Model performance:DL:train AUC = ${trainMetricvalues.auc} test  AUC = ${testMetricsvalues.auc}""".stripMargin)

       // Test the arrest rate probability for a new Crime.
    val crimeExamples = Seq(
      Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", "STREET",false, 422, 4, 7, 46, 18),
      Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", "RESIDENCE",false, 923, 9, 14, 63, 11))

    for (crime <- crimeExamples) {
      val arrestval = 100*scoreEvent(crime,nnModel,socioeconomiccensus,health)(sqlc, hc)
      println(
        s"""
           |Crime: $crime
           |  Probability of arrest best on DeepLearning: ${arrestval} %
        """.stripMargin)
    }
      }
       
       
       

   /* def load(dataset: String, modifyParserSetup: ParseSetup => ParseSetup = identity[ParseSetup]): H2OFrame = {
      val uri = java.net.URI.create(dataset)
      val parseSetup = modifyParserSetup(water.fvec.H2OFrame.parserSetup(uri))
      new H2OFrame(parseSetup, new java.net.URI(dataset))
      }

    def socioeconomiccensusdata(dataset: String): H2OFrame = {
    val socioeconomiccensusdataval = load(dataset)
    socioeconomiccensusdataval
  }

  def healthdata(dataset: String): H2OFrame = {
    val healthdataval = load(dataset)
    healthdataval
  }

  def crimedata(dataset: String): H2OFrame = {
  val crimedataval = load(dataset) 
  crimedataval
  }*/
}

