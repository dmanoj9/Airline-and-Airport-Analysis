# Airline-and-Airport-Analysis
Data: https://drive.google.com/drive/u/0/my-drive
![alt text](/sample_images/bg1.JPG)

# Problem Statement
To identify Northwoods Airline biggest competitors using the dataset provided. The dataset contains information about the Airline, Airport and Flight information which must be loaded into the cloud environment to further perform analysis.

# Objective
* The objective of this project is to create the necessary tables in snowflakes using tooling provided in Databricks or snowflakes. 
* To perform ETL using snowflake Connectors. 
* Loading the data from shared location and finally creating views and gathering the meaningful insights from the reports. 

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
