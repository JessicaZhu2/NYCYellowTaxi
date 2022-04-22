import org.apache.spark.sql.Column
import scala.io.Source
import java.io.File
import org.apache.spark.sql.functions._
import sqlContext.implicits._
import org.apache.spark.sql.DataFrame
import spark.implicits._
import org.apache.spark.sql.types._

// Set aside my user-specific particulars
val uwNetId = "vdcadena"
val topicName = uwNetId

// The below must match the Azure Event Hub you were assigned to and also
// need to be in the configuration file for the stream generator.
val bootstrapServers = "BOOTSTRAP_SERVERS_SECRET"
val saslJaasConfig = "SASLJASS_CONFIG_SECRET";"

// Create a Kafka Stream from an Azure Event Hub
// Set up a streaming DataFrame for the initial Kafka stream
import org.apache.spark.sql.functions._

val kafkaStream = spark.readStream
.format("kafka")
.option("kafka.bootstrap.servers", bootstrapServers)
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.mechanism", "PLAIN")
.option("kafka.sasl.jaas.config", saslJaasConfig)
.option("subscribe", uwNetId)
.load

// Inspect the Schema of the Kafka Stream
kafkaStream.printSchema

// Create a Schema for the JSON Records in the Stream
val jsonEventSchema = StructType(Seq(
StructField("VendorID", StringType, true),
StructField("tpep_pickup_datetime", StringType, true),
StructField("tpep_dropoff_datetime", StringType, true),
StructField("passenger_count", StringType, true),
StructField("trip_distance", StringType, true),
StructField("RatecodeID", StringType, true),
StructField("store_and_fwd_flag", StringType, true),
StructField("PULocationID", StringType, true),
StructField("DOLocationID", StringType, true),
StructField("payment_type", StringType, true),
StructField("fare_amount", StringType, true),
StructField("extra", StringType, true),
StructField("mta_tax", StringType, true),
StructField("tip_amount", StringType, true),
StructField("tolls_amount", StringType, true),
StructField("improvement_surcharge", StringType, true),
StructField("total_amount", StringType, true),
StructField("congestion_surcharge", StringType, true),
// added columns for time-windows
StructField("simulated_pickup_timestamp", StringType, true),
StructField("simulated_dropoff_datetime", StringType, true)
  )
)

// Massaging the stream into a useful form
// Create a DF that selects out the columns key, timestamp, and value from the default Kafka stream schema
// Get key, timestamp and value
val data = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(timestamp AS STRING)", "CAST(value AS STRING)")
data.printSchema
display(data)


// Create a DataFrame from the one you created in (a) that transforms the binary `value` column into a struct using Spark’s `from_json` method7.
// Name the new column “taxiTripRecord”.
val data2 = data.select(col("key").cast("string"), $"timestamp".cast("string"), from_json(col("value"), jsonEventSchema).alias("taxiTripRecord"))
data2.printSchema

val taxiTripDF = data2.select($"taxiTripRecord.*")
.withColumn("VendorID", col("VendorID").cast(IntegerType))
.withColumn("passenger_count", col("passenger_count").cast(IntegerType))
.withColumn("trip_distance", col("trip_distance").cast(DoubleType))
.withColumn("RatecodeID", col("RatecodeID").cast(IntegerType))
.withColumn("PULocationID", col("PULocationID").cast(IntegerType))
.withColumn("DOLocationID", col("DOLocationID").cast(IntegerType))
.withColumn("payment_type", col("payment_type").cast(IntegerType))
.withColumn("fare_amount", col("fare_amount").cast(DoubleType))
.withColumn("extra", col("extra").cast(DoubleType))
.withColumn("mta_tax", col("mta_tax").cast(DoubleType))
.withColumn("tip_amount", col("tip_amount").cast(DoubleType))
.withColumn("tolls_amount", col("tolls_amount").cast(DoubleType))
.withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType))
.withColumn("total_amount", col("total_amount").cast(DoubleType))
.withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType))
// convert all timestamp columns to timestamp types and calculate trip_druation
.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"),"yyyy-MM-dd HH:mm:ss"))
.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"),"yyyy-MM-dd HH:mm:ss"))
.withColumn("simulated_pickup_timestamp", to_timestamp(col("simulated_pickup_timestamp"),"yyyy-MM-dd HH:mm:ss"))
.withColumn("simulated_dropoff_datetime", to_timestamp(col("simulated_dropoff_datetime"),"yyyy-MM-dd HH:mm:ss"))
.withColumn("duration_seconds", col("tpep_dropoff_datetime").cast(LongType)-col("tpep_pickup_datetime").cast(LongType))
.withColumn("duration_mins",round(col("duration_seconds")/60))
.drop("duration_seconds")

val rawDataframeDF = taxiTripDF.select($"VendorId", $"RatecodeID", $"payment_type", $"tpep_dropoff_datetime", $"tpep_pickup_datetime")
display(rawDataframeDF)

display(taxiTripDF.select($"simulated_pickup_timestamp", $"simulated_dropoff_datetime", $"duration_mins"))

// Create Data Mapping
// Use data dictionary for Yellow Taxi Trip Records to map number ids to respective values
// https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
// data transformation function to join the mapping df to the main df
def join_datamapping(main_col: String, other_df: DataFrame, other_col: String)(main_df: DataFrame): DataFrame = {
  val joined = main_df.join(other_df, col(main_col) === col(other_col), "left").drop(col(main_col)).drop(col(other_col))
  joined
}

// A code indicating the TPEP provider that provided the record.
var data = Seq((1, "Creative Mobile Technologies"), (2, "VeriFone Inc"))
var rdd = spark.sparkContext.parallelize(data)
var vendorDF = rdd.toDF("vendor_id","vendor_name")

// The final rate code in effect at the end of the trip
data = Seq((1, "Standard rate"), (2, "JFK"), (3, "Newark"), (4, "Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride"))
rdd = spark.sparkContext.parallelize(data)
var rateCodeDF = rdd.toDF("rate_code_id","rate_type")

// A numeric code signifying how the passenger paid for the trip.
data = Seq((1, "Credit card"), (2, "Cash"), (3, "No charge"), (4, "Dispute"), (5, "Unknown"), (6, "Voided trip"))
rdd = spark.sparkContext.parallelize(data)
var paymenttypeDF = rdd.toDF("paymet_id","payment")


// // dataframe with the actual values
val mainDF = taxiTripDF.transform(join_datamapping("VendorId", vendorDF,  "vendor_id"))
                .transform(join_datamapping("RatecodeID", rateCodeDF,  "rate_code_id"))
                .transform(join_datamapping("payment", paymenttypeDF,  "paymet_id"))
//display(mainDF)
mainDF.printSchema

val cleanedDF = mainDF.select($"vendor_name", $"rate_type", $"payment_type", $"tpep_pickup_datetime", $"tpep_dropoff_datetime", $"duration_mins")
display(cleanedDF)

// Simple Queries on the Stream
// Do an aggregation on the stream to count the number of rides grouped by passenger_count
val numOfRidesDf = mainDF.groupBy("passenger_count").count()
.withColumnRenamed("count", "rides_count")
.orderBy("passenger_count")

display(numOfRidesDf)

// Distribution of Ride Duration, Distance, payment type, amount charged

// Time Windows
// Pickup Timstamp Windowed Query
val streamWithWatermark = mainDF.withWatermark("simulated_pickup_timestamp", "60 minutes").groupBy($"passenger_count", window($"simulated_pickup_timestamp", "30 minutes", "10 minutes")).count
display(streamWithWatermark)

// Structured Streaming Sinks
// Sink to a Delta Table

val storageAccount = STORAGE_ACCOUNT_SECRET"
val rootPath = s"abfss://$uwNetId@$storageAccount.dfs.core.windows.net/"
val nyctaxiPath = rootPath + "delta_for_streaming"

kafkaStream.writeStream
           .format("delta")
           .option("path", nyctaxiPath)
           .outputMode("append")
           .option("checkpointLocation", nyctaxiPath)
           .table("streaming_nyc_yellow_taxi_trips")
