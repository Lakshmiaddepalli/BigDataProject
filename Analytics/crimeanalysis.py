pyspark
from pyspark.sql import SQLContext
from pyspark.sql import Row

lines =sc.textFile("hdfs:///user/sla410/crimedatabigdataproject/crimefinal.csv")
values = lines.map(lambda v: v.split(","))
Crimes = values.map(lambda x:Row(ID =x[0],CASE_NUMBER=x[1],DATE=x[2],BLOCK=x[3],IUCR=x[4],
		PRIMARY_TYPE=x[5],DESCRIPTION=x[6],Location_DESCRIPTION =x[7],ARREST=x[8],
		DOMESTIC= x[9],BEAT=x[10],DISTRICT=x[11],WARD=x[12],COMMUNITY=x[13],FBICODE=x[14],
		XCOR=x[15],YCOR=x[16],YEAR=x[17],UPDATED_ON=x[18],LATTITUDE=x[19],LONGITUDE=x[20],
		LOCATION=x[21]))  
schemacrime=sqlContext.createDataFrame(Crimes)
schemacrime.registerTempTable("Crimes")

result=sqlContext.sql("select PRIMARY_TYPE,count(PRIMARY_TYPE) as count from Crimes group by PRIMARY_TYPE")
result.show()

+--------------------+-------+                                                  
|        PRIMARY_TYPE|  count|
+--------------------+-------+
|       OTHER OFFENSE| 425261|
|   WEAPONS VIOLATION|  72755|
|    PUBLIC INDECENCY|    164|
|NON-CRIMINAL (SUB...|      9|
|  DECEPTIVE PRACTICE| 270341|
|   HUMAN TRAFFICKING|     56|
|            BURGLARY| 391786|
|             BATTERY|1249847|
|           OBSCENITY|    602|
|             ROBBERY| 258698|
| MOTOR VEHICLE THEFT| 317807|
|        PROSTITUTION|  68575|
|     CRIMINAL DAMAGE| 780823|
|          KIDNAPPING|   6729|
|            GAMBLING|  14438|
|LIQUOR LAW VIOLATION|  14133|
|        Primary Type|      1|
|   DOMESTIC VIOLENCE|      1|
|PUBLIC PEACE VIOL...|  48338|
| CRIM SEXUAL ASSAULT|  27858|
+--------------------+-------+