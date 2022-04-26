# Data Pipeline Application for the NYC Yellow Taxi CSV files

Final Project for Big Data Class at University of Washington
By Jessica Zhu and Victor Cadena

- [Data Introduction](#data-introduction)
  * [Data Introduction: CSV files](#data-introduction--csv-files)
  * [Data Introduction: Dataframe](#data-introduction--dataframe)
- [Objective of Project](#objective-of-project)
- [Architecture](#architecture)
  * [Architecture Framework](#architecture-framework)
  * [NYC Taxi Cab Ingestion Architecture](#nyc-taxi-cab-ingestion-architecture)
- [Streaming](#streaming)
- [Data Cleaning](#data-cleaning)
- [EDA](#eda)
  * [EDA: Passenger Count Distribution](#eda--passenger-count-distribution)
  * [EDA: Ride Distance Distribution](#eda--ride-distance-distribution)
  * [EDA: Ride Duration Distribution](#eda--ride-duration-distribution)
  * [EDA: Distribution of Payment Types](#eda--distribution-of-payment-types)
  * [EDA: Distribution of Amount Charged to Passenger](#eda--distribution-of-amount-charged-to-passenger)
- [Time Windows](#time-windows)
- [Delta Table](#delta-table)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>

## Data Introduction

We chose to work on the NYC Taxi Cab Trip data from the source: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Included data for different types of taxi such as yellow taxi and green taxi.
Green taxi has some restrictions on where they are allowed to be hailed
For simplicity we are going to be focusing on the yellow taxi

The yellow taxi trip records include fields
* pick-up and drop-off dates/times
* pick-up and drop-off locations
* trip distances
* itemized fares
* rate types
* payment types
* passenger counts

### Data Introduction: CSV files

![Alt text](./graphs/data_intro.png?raw=true "Title")


### Data Introduction: Dataframe

![Alt text](./graphs/dataframe.png?raw=true "Title")


## Objective of Project

* Build a data pipeline application for the NYC Yellow Taxi CSV files
* Simulate a stream from CSV using Kafka as if it were pouring live
* Once data is streamed in Event Hub topic, use structured streaming to read/analyze data
* Create some visualizations to understand the data
  * Grouping certain columns (such as passenger_count) to do a count aggregation
  * Event-Time Windowed Query
* Sinking to a Delta Table


## Architecture

### Architecture Framework
![Alt text](./graphs/architecture.png?raw=true "Title")
### NYC Taxi Cab Ingestion Architecture
![Alt text](./graphs/nyc_taxi_cab_ingestion_architecture.png?raw=true "Title")


## Streaming
![Alt text](./graphs/process_to_stream.png?raw=true "Title")

![Alt text](./graphs/producer_high_level_code.png?raw=true "Title")

## Data Cleaning

![Alt text](./graphs/default_schema_of_kafka_stream.png?raw=true "Title")

Here is the default format of the streamed DataFrame that has default schema of ‘key’, ‘timestamp’, and ‘value’ columns from the Kafka stream

![Alt text](./graphs/creating_df_of_value_column.png?raw=true "Title")

Now we are using the default’s dataframe binary ‘value’ column to create a new DataFrame.

This can be done by 
* First creating a schema for the json record in the stream
 * For some reason, the schema was all in StringType so each StructField had to be a StringType
* Then we transform the binary ‘value’ column into the struct using the from_json method
* Finally, we select all column that were specified in our struct and casted each column to the appropriate data type


![Alt text](./graphs/data_mapping_wrangling.png?raw=true "Title")


Lastly, for our data cleaning:

* Since some of the columns were coded with a numerical score,
 * We had to do some mapping based off the data dictionary to get descriptive meaning
* Also after converting our pickup and dropoff times to Spark timestamp, we created another column for the taxi ride’s duration in minutes

* VendorID: A code indicate the provider that provided the record
* RatecodeId: final rate code at the  end of trip
* Duration_mins: calculate by getting the dropoff time - pickup time

## EDA


Now we will look at some graphs we did for our Exploratory data analysis


### EDA: Passenger Count Distribution
![Alt text](./graphs/ride_counts.png?raw=true "Title")


Aggregation on the stream to group by the passenger count

* As we can majority of the rides had only one passenger - at this point of the stream 700K rides
* Two passenger was the second most popular
* Remaining passenger count’s rides had least amount of rides


### EDA: Ride Distance Distribution
![Alt text](./graphs/distribution_of_ride_distance.png?raw=true "Title")

Here we look at the distribution of the ride distance. 
* This is the distribution of the ride duration. 

Like the Ride Distance distribution, it is also is Right-Skewed

This means that most of the rides are short distance and are less than 20 mins
* Majority of the rides are under 2 or 3 miles


### EDA: Ride Duration Distribution
![Alt text](./graphs/distribution_of_ride_duration.png?raw=true "Title")

This is the distribution of the ride duration. 

Like the Ride Distance distribution, it is also is Right-Skewed

* This means that most of the rides are short distance and are less than 20 mins


### EDA: Distribution of Payment Types
![Alt text](./graphs/payment_type.png?raw=true "Title")

This is the distribution of payment types

* We can see that most of the customer’s paid using a credit card of about 78%.
* The second most popular type of payment was cash with 21%.
* Very small percentage remains for the remaining two payment types: No charge & dispute


### EDA: Distribution of Amount Charged to Passenger
![Alt text](./graphs/distribution_of_amount_charged.png?raw=true "Title")

Like the other previous distribution, it is also is Right-Skewed

* So we can see most of the passengers were charged about $15 
* With exponentially less and less passenger being charged more than that

## Time Windows
![Alt text](./graphs/timestamp.png?raw=true "Title")

## Delta Table
![Alt text](./graphs/sink_to_delta_table.png?raw=true "Title")


