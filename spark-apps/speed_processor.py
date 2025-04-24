# ./spark-apps/speed_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType # Add more as needed
import os

# Kafka details
KAFKA_BROKER = "kafka:9092" # Internal Kafka address
KAFKA_TOPIC = "flight_data_raw"
# Elasticsearch details
ES_HOST = "elasticsearch"
ES_PORT = "9200"
ES_INDEX_SPEED = "flight_summary_speed" # Different index for speed results
ES_CHECKPOINT_LOCATION = f"/tmp/spark-checkpoint-speed-{ES_INDEX_SPEED}" # Checkpoint location *inside container*
ES_NODES = f"http://{ES_HOST}:{ES_PORT}"

spark_master = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")

print(f"Starting Spark Streaming session connected to: {spark_master}")
print(f"Reading stream from Kafka: {KAFKA_BROKER}/{KAFKA_TOPIC}")
print(f"Writing results to Elasticsearch: {ES_NODES}/{ES_INDEX_SPEED}")

try:
    spark = SparkSession.builder \
        .appName("Flight Speed Processor") \
        .master(spark_master) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4") \
        .config("spark.sql.streaming.checkpointLocation", ES_CHECKPOINT_LOCATION) \
        .config("spark.driver.memory", "750m") \
        .config("spark.executor.memory", "750m") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.es.nodes", ES_HOST) \
        .config("spark.es.port", ES_PORT) \
        .config("spark.es.nodes.wan.only", "false") \
        .config("spark.es.resource", f"{ES_INDEX_SPEED}") \
        .getOrCreate()

    print("Spark session created successfully.")

    # Define schema matching the JSON structure sent to Kafka
    # Important: Match the data types and field names from your CSV/producer
    schema = StructType([
    StructField("Year", StringType(), True), # Or IntegerType() if always numeric
    StructField("Quarter", StringType(), True), # Or IntegerType()
    StructField("Month", StringType(), True), # Or IntegerType()
    StructField("DayofMonth", StringType(), True), # Or IntegerType()
    StructField("DayOfWeek", StringType(), True), # Or IntegerType()
    StructField("FlightDate", StringType(), True), # Keep as String unless parsing needed
    StructField("Reporting_Airline", StringType(), True),
    StructField("DOT_ID_Reporting_Airline", StringType(), True), # Or IntegerType()
    StructField("IATA_CODE_Reporting_Airline", StringType(), True),
    StructField("Tail_Number", StringType(), True),
    StructField("Flight_Number_Reporting_Airline", StringType(), True), # Or IntegerType()
    StructField("OriginAirportID", StringType(), True), # Or IntegerType()
    StructField("OriginAirportSeqID", StringType(), True), # Or IntegerType()
    StructField("OriginCityMarketID", StringType(), True), # Or IntegerType()
    StructField("Origin", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("OriginState", StringType(), True),
    StructField("OriginStateFips", StringType(), True), # Or IntegerType()
    StructField("OriginStateName", StringType(), True),
    StructField("OriginWac", StringType(), True), # Or IntegerType()
    StructField("DestAirportID", StringType(), True), # Or IntegerType()
    # ... CONTINUE FOR ALL REMAINING FIELDS ...
    StructField("DepDelayMinutes", DoubleType(), True),
    StructField("DepDel15", DoubleType(), True), # Or IntegerType()
    StructField("DepartureDelayGroups", StringType(), True), # Or IntegerType()
    StructField("DepTimeBlk", StringType(), True),
    StructField("TaxiOut", DoubleType(), True),
    StructField("WheelsOff", StringType(), True), # Time, keep as String
    StructField("WheelsOn", StringType(), True),  # Time, keep as String
    StructField("TaxiIn", DoubleType(), True),
    StructField("CRSArrTime", StringType(), True), # Time, keep as String
    StructField("ArrTime", StringType(), True), # Time, keep as String
    StructField("ArrDelay", DoubleType(), True),
    StructField("ArrDelayMinutes", DoubleType(), True),
    StructField("ArrDel15", DoubleType(), True), # Or IntegerType()
    StructField("ArrivalDelayGroups", StringType(), True), # Or IntegerType()
    StructField("ArrTimeBlk", StringType(), True),
    StructField("Cancelled", DoubleType(), True), # Check if 0.0/1.0 or 0/1 -> IntegerType maybe?
    StructField("CancellationCode", StringType(), True),
    StructField("Diverted", DoubleType(), True), # Check if 0.0/1.0 or 0/1 -> IntegerType maybe?
    StructField("CRSElapsedTime", DoubleType(), True),
    StructField("ActualElapsedTime", DoubleType(), True),
    StructField("AirTime", DoubleType(), True),
    StructField("Flights", DoubleType(), True), # Probably IntegerType? Check data
    StructField("Distance", DoubleType(), True),
    StructField("DistanceGroup", StringType(), True), # Or IntegerType()
     # Add fields for delay reasons if they exist and are needed
    StructField("CarrierDelay", DoubleType(), True),
    StructField("WeatherDelay", DoubleType(), True),
    StructField("NASDelay", DoubleType(), True),
    StructField("SecurityDelay", DoubleType(), True),
    StructField("LateAircraftDelay", DoubleType(), True),
    # Add fields for diverted legs if they exist and are needed (many might be null)
    # ... all the Div1... Div5... fields ... they are mostly StringType or DoubleType for times
])

    # Read from Kafka stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON value from Kafka
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")

    # Add processing timestamp (useful for windowing)
    # json_df = json_df.withColumn("processingTime", current_timestamp())

    # Perform Streaming Analysis (Example: Count flights per origin city in 1-minute windows)
    # Need a timestamp column for windowing. Let's assume 'FlightDate' + 'DepTime' can be parsed,
    # or use processing time. Using a placeholder for now.
    # If using event time, ensure data has timestamps and handle watermarking.
    # For simplicity here, let's just count recent events without windowing directly to ES.

    # Example: Simple count per Origin City (updates frequently)
    origin_counts = json_df.groupBy("OriginCityName") \
                           .count() \
                           .withColumnRenamed("count", "recent_flight_count") \
                           .withColumn("id", col("OriginCityName")) # Use OriginCityName as ES doc ID

    # Write stream to Elasticsearch
    # Use 'update' mode for streaming aggregates if IDs conflict, or 'append' for raw events.
    # For counts with unique IDs per group, 'complete' output mode might work with foreachBatch.
    # Using foreachBatch is more flexible for complex updates/writes.

    def write_to_es(df_batch, batch_id):
         print(f"Writing batch {batch_id} to ES index {ES_INDEX_SPEED}...")
         df_batch.write \
             .format("org.elasticsearch.spark.sql") \
             .option("es.resource", f"{ES_INDEX_SPEED}") \
             .option("es.nodes", ES_HOST) \
             .option("es.port", ES_PORT) \
             .option("es.mapping.id", "id") \
             .mode("append")\
             .save()
         print(f"Finished writing batch {batch_id}.")

    # Using append mode here, assuming each record is processed individually
    # For aggregation like counts, need a different approach (e.g., update in ES)
    # query = origin_counts.writeStream \
    #     .outputMode("update") # 'update' or 'complete' for aggregations
    #     .foreachBatch(write_to_es) \
    #     .option("checkpointLocation", ES_CHECKPOINT_LOCATION + "_agg") \
    #     .start()

    # Let's just write the raw (parsed) stream data to ES for simplicity
    query = json_df.withColumn("id", expr("uuid()")) \
         .writeStream \
         .outputMode("append") \
         .foreachBatch(write_to_es) \
         .option("checkpointLocation", ES_CHECKPOINT_LOCATION + "_raw") \
         .trigger(processingTime='30 seconds')\
         .start()


    print("Streaming query started. Waiting for termination...")
    query.awaitTermination()

except Exception as e:
    print(f"An error occurred during streaming processing: {e}")
    import traceback
    traceback.print_exc()

finally:
    if 'spark' in locals() and 'query' in locals() and query.isActive:
         query.stop()
    if 'spark' in locals():
        spark.stop()
        print("Spark session stopped.")