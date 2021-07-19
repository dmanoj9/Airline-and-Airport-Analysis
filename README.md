# Airline-and-Airport-Analysis

![alt text](/sample_images/bg1.JPG)

# Problem Statement
To identify Northwoods Airline biggest competitors using the dataset provided. The dataset contains information about the Airline, Airport and Flight information which must be loaded into the cloud environment to further perform analysis.

# Objective
* The objective of this project is to create the necessary tables in snowflakes using tooling provided in Databricks or snowflakes. 
* To perform ETL using snowflake Connectors. 
* Loading the data from shared location and finally creating views and gathering the meaningful insights from the reports. 

# Data
https://drive.google.com/file/d/1S-32SHBuw0Y_iVI20XaW88iYEd7EgEog/view?usp=sharing

## Airline 

| Column Name  | Data Type | Description|
| ------------- | ------------- | ------------- |
|IATA_CODE  | String | Airline Identifier |
|AIRLINE  | String  | Airline name |

## Airport 

| Column Name  | Data Type | Description|
| ------------- | ------------- | ------------- |
|IATA_CODE  | String | Airline Identifier |
|AIRPORT  | String  | Airport name |
|CITY  | String |City where the airport is located |
|STATE  | String  | State where the airport is located |
|COUNTRY  | String  | The country where the airport is located |
|LATITUDE  | Number| Latitude of airport |
|LONGITUDE  | Number  | Longitude of airport|

## Flights 

| Column Name  | Data Type | Description|
| ------------- | ------------- | ------------- |
|YEAR  | Number  | Year of the Flight |
|MONTH  | Number  | Month of the Flight |
|DAY  | Number  | Day of the Flight |
|DAY_OF_WEEK  | Number  | Day of the of the Flight |
|AIRLINE  | String  | Airline Identifier |
|FLIGHT_NUMBER  | String  | Flight Identifier |
|TAIL_NUMBER  | String  | Aircraft Identifier |
|ORIGIN_AIRPORT  | String  | Starting airport |
|DESTINATION_AIRPORT  | String  | Destination airport |
|SCHEDULED_DEPARTURE  |   | Planned departure time|
|DEPARTURE_TIME  | String  |Wheels up time |
|DEPARTURE_DELAY  | Number  |Total delay on departure |
|TAXI_OUT  | Number  |The time duration elapsed between departure from the origin airport gate and wheels off |
|WHEELS_OFF  | String  | The time point that the aircraft's wheels leave the ground |
|SCHEDULED_TIME  | Number  | Planned time amount needed for the flight trip |
|ELAPSED_TIME  | Number  | Total trip time |
|AIR_TIME  | Number  |The time duration between wheels_off and wheels_on time |
|DISTANCE  | Number  | The distance between two airports|




# Workflow
The workflow which I followed for building this project. 
![](/sample_images/image%202.JPG)

# Scala Code
"Snowflake connector Scala notebook" is the databricks notebook where connectors are used to connect to snowflake from databricks notebook.

# View
Here SQL queries are written to execute the following views:

* Total number of flights by airline and airport on a monthly basis
* On time percentage of each airline for the year 2015
* Airlines with the largest number of delays
* Cancellation reasons by airport
* Delay reasons by airport
* Airline with the most unique routes

# Future Scope 

* WN and DL airlines are our biggest competitors and since airports like ATL and ORD have the largest total number of flights by airport we should consider those locations to be our target origin airports.
* It is not just important to have highest number of flights, but we must also   maintain the consistency in increasing the percentage of being on time and maintaining a smaller number of delays. This can be achieved by constantly analyzing these metrics and identifying where there is lag. 
* Weather plays a major role in the cancellation of Flights; it is seen that highest number of flights have been cancelled due to reason Weather(B) To avoid the inconvenience passengers should be informed earlier about the flight cancellation by looking at the weather forecast. 
* Delays caused by Airport was highest at ATL airport and 3rd highest at ORD airport. Majority of delays by airport were only caused by Air System of the Airports. We can overcome this by pushing the boarding time earlier so the flight can get ready for take off as per the estimated departure timing.
* Flying flights in Unique routes can be tricky to gain profits, but we can over come this by choosing either the origin or the destination airports which are busiest among all to get more flyers.

