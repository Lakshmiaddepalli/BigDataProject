import org.apache.spark.sql.functions._
var threshold = df2.select(avg($"PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA")).show()

var CommunityWisePrimaryTypeAndIllitracy = df3.groupBy($"Community Area",$"Primary Type",$"PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA").count().orderBy(desc("count"))

CommunityWisePrimaryTypeAndIllitracy.where($"PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA"> 20.33076923076923).show()


Community Area|   Primary Type|PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA|count|
+--------------+---------------+--------------------------------------------+-----+
|            25|      NARCOTICS|                                        24.4|11723|  //25 ---> Austin
|            25|        BATTERY|                                        24.4| 8410|
|            25|          THEFT|                                        24.4| 6216|
|            23|      NARCOTICS|                                        35.4| 5163|
|            67|        BATTERY|                                        26.3| 4723|
|            23|        BATTERY|                                        35.4| 4609|
|            29|      NARCOTICS|                                        27.6| 4571|
|            29|        BATTERY|                                        27.6| 4539|
|            68|        BATTERY|                                        28.5| 4415|
|            25|CRIMINAL DAMAGE|                                        24.4| 3826|
|            66|        BATTERY|                                        31.2| 3650|
|            26|      NARCOTICS|                                        24.5| 3303|
|            67|          THEFT|                                        26.3| 3252|
|            67|      NARCOTICS|                                        26.3| 3117|
|            61|        BATTERY|                                        41.5| 3116|
|            66|      NARCOTICS|                                        31.2| 3104|
|            23|          THEFT|                                        35.4| 2967|
|            46|        BATTERY|                                        26.6| 2950|
|            25|       BURGLARY|                                        24.4| 2925|
|            66|          THEFT|                                        31.2| 2919


The 3 types of Primary Area for People aged 25+ without high scool diploma are "Narcotics, Battery, and Theft" and the highest place of
crimes occured in Austin where the percentage of illitracy was above  the threshold 20.33.
