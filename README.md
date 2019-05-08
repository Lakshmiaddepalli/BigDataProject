# BigDataProject
CSCI-GA.3033-005 - Big Data Application Development


## We are using the following data to analyse crime in Chicago and best places to live in Chicago. we have taken into account the following datasets

1. Crimes (Presently Used)[https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2]
2. Socioeconomic_Indicators (Presently Used) https://data.cityofchicago.org/Health-Human-Services/Census-Data-Selected-socioeconomic-indicators-in-C/kn9c-c2s2
3. Public Health Statistics- Selected public health indicators by Chicago community area(Presently Used) https://data.cityofchicago.org/Health-Human-Services/Public-Health-Statistics-Selected-public-health-in/iqnk-2tcu
4. Crimes Description (Presently Used) https://data.cityofchicago.org/Public-Safety/Chicago-Police-Department-Illinois-Uniform-Crime-R/c7ck-438e
5. Sex Offenders (Presently Used) https://data.cityofchicago.org/Public-Safety/Sex-Offenders/vc9r-bqvy
6. neighbourhoods (shape file) (Presently Used) [https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas-current-/cauq-8yn6]
7. Food Inspection (Presently Used) https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5
8. Affordable Rental Housing (Presently Used) https://data.cityofchicago.org/Community-Economic-Development/Affordable-Rental-Housing-Developments/s6ha-ppgi
9. 311 Service Requests - Vacant and Abandoned Buildings Reported.csv (Future Work) https://data.cityofchicago.org/Service-Requests/311-Service-Requests-Vacant-and-Abandoned-Building/7nii-7srd

All the data can be found on https://data.cityofchicago.org/

## Acknowledgement: 

1. The code to convert latitude and longitude to community area was ideated from this:  
https://github.com/craigmbooth/chicago_neighborhood_finder

2. A Google Maps geocoding library for Scala
https://github.com/KoddiDev/geocoder


## Team Members:

1. Sree Lakshmi Addepalli
2. Divya Juneja
3. Sree Gowri Addepalli


##  Folders and process to run ...

# 01. Data
It contains the datasets used for the project. Some might not be present as they  are  greater than 25 MB

# 02. DataIngestion
It  contains the scala code for ingesting the datasets into the HDFS.

# 03. Preprocessing
It  contains the scala code for preprocessing the datasets. The following methods are done in preprocessing.

The Community Area is an important part of the dataset as it gives values community area wise.After getting the latitude and longitude of the rows which had missing values we applied two procedures:

i.Used the communities areas shape file dataset given by Chicago government and generated a json for range of geopoints for each community. Later we tagged the unknown community area by passing the latitude and longitude.

ii.Still there were few values giving none and not being detected any community. We further went ahead and generated a script which calculated the Euclidean Squared Distance Metric from each community area center and later the nearest community area was set to this row.

# 04. Cleaning and Profiling
The following steps are used for cleaning and preprocessing:
a. Fill missing values: Ignore/drop the rows having missing values.
                        If the column is numerical, filled in the missing value with the mean/avg value of the column. 
                        If the column is categorical, fill in with the most occurring category
b. Feature Engineering: input data was used to derive new features.(Example Date)
                        withColumn was used for adding/replacing an existing column
                        3 udfs were used to generate new features
                        Binning / Bucketing was used to generate new features
c. Feature Selection: Correlation analysis was done to get the interdependence between variables and to  drop columns which are highly dependent based on pvalue.


# 05. Profiling - Mentioned above

# 06. Analytics
The following Analytics was done: 

Analytics of CrimeData

Analytics Between Poverty and Arrests

CommunityWise Crime Analysis and Housing Crowded

CommunityWise Primary Crime Income and Unemployment

CommunityWise Primary Crime and Hardship

Community Wise Primary Type And Illiteracy

Community Wise Total Vacant Buildings

07. Visualisation
08. DL_Algorithm
09. Outputs
10. dataextraction
11. Remediation_Code
12. Extras
13. issues.md
14. Papers
15. Bitcoin
