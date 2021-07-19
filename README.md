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

