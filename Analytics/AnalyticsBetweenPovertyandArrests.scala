//1. Joining economic conditions with crime in each community

import org.apache.spark.sql.functions._
var threshold = df2.select(avg($"PERCENT HOUSEHOLDS BELOW POVERTY"))



var df3 = df.join(df2, Seq("Community Area"), "left_outer")
var PovertyRateandArrestsNotMade = df3.groupBy($"Arrest",$"PERCENT HOUSEHOLDS BELOW POVERTY").count().orderBy(desc("count"))
PovertyRateandArrestsNotMade.collect().foreach(println)

//Got Community Area wise Arrests not made and its relation with PERCENT HOUSEHOLDS BELOW POVERTY
var CommunityWisePovertyRateandArrestsNotMade = df3.groupBy($"Community Area",$"Arrest",$"PERCENT HOUSEHOLDS BELOW POVERTY").count().orderBy(desc("count"))
CommunityWisePovertyRateandArrestsNotMade.collect().foreach(println)

//It could be seen that the top 5 Areas were where the arrests were not made and 3 out of 5 had a had high percentage of poverty above the threshold value 21.739743589743593.
[25,false,28.6,317]   //Austin                                                          
[8,false,12.9,235]    //Near North Side
[43,false,31.1,217]   //South Shore
[23,false,33.9,169]   //Humboldt Park
[24,false,14.7,169]   //West Town


//Austin already was high on criminal activity but we could see that the poverty line was also above threshold