// Databricks notebook source
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// DBTITLE 1,Snowflake connection string
val options = Map(
  "sfUrl" -> "pq23550.us-east-2.aws.snowflakecomputing.com",
  "sfUser" -> "manojd",
  "sfPassword" -> "*******",
  "sfDatabase" -> "AIR_TRAVEL",
  "sfSchema" -> "PUBLIC",
  "sfWarehouse" -> "NORTHWOODS_DWH_01"
)

// COMMAND ----------

// DBTITLE 1,Reading Local Tables Data into respective dataframes
// Getting Flight Data
val flight_data=spark.read.format("csv")
.option("header","true")
.option("inferSchema",true)
.load("/FileStore/tables/flight/*.csv")

//Getting Airports Data
val airports_data=spark.read.format("csv")
.option("header","true")
.option("inferSchema",true)
.load("FileStore/tables/airports/airports.csv")

//Getting Airlines Data
val airlines_data=spark.read.format("csv")
.option("header","true")
.option("inferSchema",true)
.load("/FileStore/tables/airlines/airlines.csv")

// COMMAND ----------

flight_data.count() //3920766
//airports_data.count() //322
//airlines_data.count() //14

// COMMAND ----------

// DBTITLE 1,Writing Flights data into Snowflake table

flight_data.write
  .format("snowflake")
  .mode(SaveMode.Overwrite) // Full Load // .mode(SaveMode.Append)
  .options(options)
  .option("dbtable", "flight")
  .save()

// COMMAND ----------

// DBTITLE 1,Writing Airport data into Snowflake table

airports_data.write
  .format("snowflake")
  .mode(SaveMode.Overwrite)// Full Load // .mode(SaveMode.Append)
  .options(options)
  .option("dbtable", "airports")
  .save()

// COMMAND ----------

// DBTITLE 1,Writing Airlines data into Snowflake table

airlines_data.write
  .format("snowflake")
  .mode(SaveMode.Overwrite) // Full Load  .mode(SaveMode.Append)
  .options(options)
  .option("dbtable", "airlines")
  .save()

// COMMAND ----------

// DBTITLE 1,TOTAL_AIR_LINES
val TOTAL_AIR_LINES_df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "V_TOTAL_AIR_LINES")
  .load()

display(TOTAL_AIR_LINES_df)


// COMMAND ----------

// DBTITLE 1,ON_TIME_PERCENT
val ON_TIME_PERCENT_df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "V_ON_TIME_PERCENT")
  .load()

display(ON_TIME_PERCENT_df)


// COMMAND ----------

// DBTITLE 1,LARGEST_NUMBER_DELAY
val LARGEST_NUMBER_DELAY_df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "V_LARGEST_NUMBER_DELAY")
  .load()

display(LARGEST_NUMBER_DELAY_df)


// COMMAND ----------

// DBTITLE 1,CANCELLATION_BY_AIRPORT

val CANCELLATION_BY_AIRPORT_df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "V_CANCELLATION_BY_AIRPORT")
  .load()

display(CANCELLATION_BY_AIRPORT_df)


// COMMAND ----------

// DBTITLE 1,DELAY_BY_AIRPORT
val DELAY_BY_AIRPORT_df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "V_DELAY_BY_AIRPORT")
  .load()

display(DELAY_BY_AIRPORT_df)

// COMMAND ----------

// DBTITLE 1,Airline_wise_unique_route
val airline_wise_unique_route_df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "V_airline_wise_unique_route")
  .load()

display(airline_wise_unique_route_df)

// COMMAND ----------

// DBTITLE 1,UNIQUE_ROUTE
val UNIQUE_ROUTE_df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "V_UNIQUE_ROUTE")
  .load()

display(UNIQUE_ROUTE_df)
