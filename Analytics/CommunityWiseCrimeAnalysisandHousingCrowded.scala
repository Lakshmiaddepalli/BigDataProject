import org.apache.spark.sql.functions._
var crowedhousingthreshold = df2.select(avg($"PERCENT OF HOUSING CROWDED")).show()
+-------------------------------+                                               
|avg(PERCENT OF HOUSING CROWDED)|
+-------------------------------+
|               4.92051282051282|
+-------------------------------+


var CommunityWisePrimaryCrimeandcrowedhousing = df3.groupBy($"Community Area",$"Primary Type",$"PERCENT OF HOUSING CROWDED").count().orderBy(desc("count"))

CommunityWisePrimaryCrimeandcrowedhousing.where($"PERCENT OF HOUSING CROWDED"> 4.92051282051282).show()


+--------------+---------------+--------------------------+-----+               
|Community Area|   Primary Type|PERCENT OF HOUSING CROWDED|count|
+--------------+---------------+--------------------------+-----+
|            25|      NARCOTICS|                       6.3|11723| ----> Austin
|            25|        BATTERY|                       6.3| 8410|
|            25|          THEFT|                       6.3| 6216|
|            23|      NARCOTICS|                      14.8| 5163|
|            23|        BATTERY|                      14.8| 4609|
|            29|      NARCOTICS|                       7.4| 4571|
|            29|        BATTERY|                       7.4| 4539|
|            25|CRIMINAL DAMAGE|                       6.3| 3826|
|            66|        BATTERY|                       7.6| 3650|
|            26|      NARCOTICS|                       9.4| 3303|
|            61|        BATTERY|                      11.9| 3116|
|            66|      NARCOTICS|                       7.6| 3104|
|            23|          THEFT|                      14.8| 2967|
|            25|       BURGLARY|                       6.3| 2925|
|            66|          THEFT|                       7.6| 2919|
|            27|      NARCOTICS|                       8.2| 2902|
|            29|          THEFT|                       7.4| 2790|
|            27|        BATTERY|                       8.2| 2740|
|            25|  OTHER OFFENSE|                       6.3| 2671|
|            19|          THEFT|                      10.8| 2605|
+--------------+---------------+--------------------------+-----+


The 3 types of Primary Area for PERCENT OF HOUSING CROWDED greater than threshold are "Narcotics, Battery, and Theft" and
 it happens in Austin where the PERCENT OF HOUSING CROWDED  = 6.3