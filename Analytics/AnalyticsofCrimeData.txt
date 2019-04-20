
//1. The type of crimes taking place sorted descending by there count
var crime_groupstypes = df.groupBy($"Primary Type").count()
crime_groupstypes.collect().foreach(println)
var crime_type_counts = crime_groupstypes.orderBy(desc("count"))
crime_type_counts.collect().foreach(println)

//2. Community Area with maximum criminal offense - Austin Comes out to be the most criminal area
var Communityareacount =  df.groupBy($"Community Area").count().orderBy(desc("count"))
Communityareacount.collect().foreach(println)
Communityareacount.take(1).foreach(println)


//3. crime types varrying arrest over years
 var typearrestdate = df.groupBy($"Arrest",$"Date_of_Crime").count().orderBy(asc("Date_of_Crime"),desc("count"))
 typearrestdate.show(3)
 
//4.The most  type of crime occuring in each community(getting issues)
var toptypeofcrimes = df.select($"Primary Type").groupBy($"Primary Type").count().rdd.map(row => (row:(1/("count")))).takeOrdered(10)
