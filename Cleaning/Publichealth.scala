spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df3 = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/sla410/crimedatabigdataproject/Public_Health_Statistics-_Selected_public_health_indicators_by_Chicago_community_area.csv")
Below Poverty Level	Crowded Housing	Dependency	No High School Diploma	Per Capita Income	Unemployment

df3.printSchema()

df3 =  df3.withColumnRenamed("Community Area", "Community_Area")
df3 =  df3.withColumnRenamed("Community Area Name", "Community_Area_Name")
df3 =  df3.withColumnRenamed("Birth Rate", "Birth_Rate")
df3 =  df3.withColumnRenamed("General Fertility Rate", "General_Fertility_Rate")
df3 =  df3.withColumnRenamed("Low Birth Weight", "Low_Birth_Weight")
df3 =  df3.withColumnRenamed("Prenatal Care Beginning in First Trimester", "Prenatal_Care_Beginning_in_First_Trimester")
df3 =  df3.withColumnRenamed("Preterm Births", "Preterm_Births")
df3 =  df3.withColumnRenamed("Teen Birth Rate", "Teen_Birth_Rate")
df3 =  df3.withColumnRenamed("Assault (Homicide)", "Assault")
df3 =  df3.withColumnRenamed("Breast cancer in females", "Breast_cancer_in_females")
df3 =  df3.withColumnRenamed("Cancer (All Sites)", "Cancer")
df3 =  df3.withColumnRenamed("Colorectal Cancer", "Colorectal_Cancer")
df3 =  df3.withColumnRenamed("Diabetes-related", "Diabetes_related")
df3 =  df3.withColumnRenamed("Firearm-related", "Firearm_related")
df3 =  df3.withColumnRenamed("Infant Mortality Rate", "Infant_Mortality_Rate")
df3 =  df3.withColumnRenamed("Lung Cancer", "Lung_Cancer")
df3 =  df3.withColumnRenamed("Prostate Cancer in Males", "Prostate_Cancer_in_Males")
df3 =  df3.withColumnRenamed("Stroke (Cerebrovascular Disease)", "Stroke")
df3 =  df3.withColumnRenamed("Childhood Blood Lead Level Screening", "Childhood_Blood_Lead_Level_Screening")
df3 =  df3.withColumnRenamed("Childhood Lead Poisoning", "Childhood_Lead_Poisoning")
df3 =  df3.withColumnRenamed("Gonorrhea in Females", "Gonorrhea_in_Females")
df3 =  df3.withColumnRenamed("Gonorrhea in Males", "Gonorrhea_in_Males")


df3.filter(df3("Community Area").isNull || df3("Community Area") === "" || df3("Community Area").isNaN).count()


df2 =  df2.withColumnRenamed("Community Area", "Community_Area")
df2 =  df2.withColumnRenamed("COMMUNITY AREA NAME", "COMMUNITY_AREA_NAME")
df2 =  df2.withColumnRenamed("PERCENT OF HOUSING CROWDED", "PERCENT_OF_HOUSING_CROWDED")
df2 =  df2.withColumnRenamed("PERCENT AGED 16+ UNEMPLOYED", "PERCENT_AGED_16+_UNEMPLOYED")
df2 =  df2.withColumnRenamed("PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA", "PERCENT_AGED 25+_WITHOUT_HIGH_SCHOOL_DIPLOMA")
df2 =  df2.withColumnRenamed("PERCENT HOUSEHOLDS BELOW POVERTY", "PERCENT_HOUSEHOLDS_BELOW_POVERTY")
df2 =  df2.withColumnRenamed("PERCENT AGED UNDER 18 OR OVER 64", "PERCENT_AGED_UNDER_18_OR_OVER_64")
df2 =  df2.withColumnRenamed("PER CAPITA INCOME ", "PER_CAPITA_INCOME")
df2 =  df2.withColumnRenamed("HARDSHIP INDEX", "HARDSHIP_INDEX")
