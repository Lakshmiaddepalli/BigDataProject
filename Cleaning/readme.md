#1. Crimes Dataset

Schema - The dates are from 2001 - present

df.printSchema()


root

 |-- ID: string (nullable = true)
 
 |-- Case Number: string (nullable = true)
 
 |-- Date: string (nullable = true)
 
 |-- Block: string (nullable = true)
 
 |-- IUCR: string (nullable = true)
 
 |-- Primary Type: string (nullable = true)
 
 |-- Description: string (nullable = true)
 
 |-- Location Description: string (nullable = true)
 
 |-- Arrest: string (nullable = true)
 
 |-- Domestic: string (nullable = true)
 
 |-- Beat: string (nullable = true)
 
 |-- District: string (nullable = true)
 
 |-- Ward: string (nullable = true)
 
 |-- Community Area: string (nullable = true)
 
 |-- FBI Code: string (nullable = true)
 
 |-- X Coordinate: string (nullable = true)
 
 |-- Y Coordinate: string (nullable = true)
 
 |-- Year: string (nullable = true)
 
 |-- Updated On: string (nullable = true)
 
 |-- Latitude: string (nullable = true)
 
 |-- Longitude: string (nullable = true)
 
 |-- Location: string (nullable = true)
 
 
 1. The date column as a string was formatted to "dd/mm/yyyy" so that it had a common nomenclature.
 
 2. In the "Location Description" column some are empty. As they are 4775 values out of 1048576 that are empty dropping them would not be great.In future when we slice and dice according to a year of data the empty value may reduce and then can be dropped. For example while slicing it from 2008-2012 range, there are only 17 rows and hence can be dropped.
 
 3. The latitudes and longitudes of about 63633 rows were missing hence to handle them we took the block address and used the google api geocoding service to get the latitude and the longitude. A script was written for the following.
 
 4.The Community Area is an important part of the dataset as it gives values community area wise.After getting the latitude and longitude of the rows which had missing values we applied two procedures:
 
i.Used the communities areas shape file dataset given by Chicago government and generated a json for range of geopoints for each community. Later we tagged the unknown community area by passing the latitude and longitude.

ii.Still there were few values giving none and not being detected any community. We further went ahead and generated a script which calculated the Euclidean Squared Distance Metric from each community area center and later the nearest community area was set to this row.

5.Columns like District(47), ward(614840), X Coordinate(63633), Y Coordinate(63633),Location(63633) play least significance in the analytics part hence can be set as it is.

6. Rest columns have 0 null values or empty values.
 
 
 
