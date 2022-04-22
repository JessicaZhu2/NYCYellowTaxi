# Exploratary Data Analysis on Depression Anxiety Stress
Final Project for Big Data Class at University of Washington
By Jessica Zhu and Victor Cadena


## Data Introduction

We chose to work on the NYC Taxi Cab Trip data

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
![Alt text](./graphs/architecture_framework?raw=true "Title")
### NYC Taxi Cab Ingestion Architecture
![Alt text](./Graphs/nyc_taxi_cab_ingestion_architecture?raw=true "Title")


## Streaming
![Alt text](./Graphs/process_to_stress?raw=true "Title")

![Alt text](./Graphs/producer_high_level_code?raw=true "Title")

## Data Cleaning

![Alt text](./Graphs/default_schema_of_kafka_stream?raw=true "Title")

![Alt text](./Graphs/creating_df_of_value_column?raw=true "Title")

![Alt text](./Graphs/data_mapping_wrangling?raw=true "Title")


## EDA



### EDA: Passenger Count Distribution
![Alt text](./Graphs/count_vs_ride_count?raw=true "Title")


### EDA: Ride Distance Distribution
![Alt text](./Graphs/distribution_of_ride_distance?raw=true "Title")

### EDA: Ride Duration Distribution
![Alt text](./Graphs/distribution_of_ride_duration?raw=true "Title")

### EDA: Distribution of Payment Types
![Alt text](./Graphs/payment_type?raw=true "Title")


### EDA: Distribution of Amount Charged to Passenger

## Time Windows
![Alt text](./Graphs/timestamp?raw=true "Title")
## Delta Table
![Alt text](./Graphs/sink_to_delta_table?raw=true "Title")

