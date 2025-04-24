# ./spark-apps/batch_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, expr
import os

# HDFS Path where the master CSV is stored
HDFS_INPUT_PATH = "hdfs://hadoop-namenode:9000/user/flight_data/flight_data_master.csv"
# Elasticsearch details
ES_HOST = "elasticsearch"
ES_PORT = "9200"
ES_INDEX = "flight_summary_batch" # Index for batch results
ES_NODES = f"http://{ES_HOST}:{ES_PORT}"

# Check if running in Docker/Cluster or local development
# In the cluster, Spark usually finds the master. Locally you might need to set it.
spark_master = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")

print(f"Starting Spark session connected to: {spark_master}")
print(f"Reading data from HDFS: {HDFS_INPUT_PATH}")
print(f"Writing results to Elasticsearch: {ES_NODES}/{ES_INDEX}")

try:
    spark = SparkSession.builder \
        .appName("Flight Batch Processor") \
        .master(spark_master) \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.es.nodes", ES_HOST) \
        .config("spark.es.port", ES_PORT) \
        .config("spark.es.nodes.wan.only", "false")\
        .config("spark.es.resource", f"{ES_INDEX}") \
        .config("spark.es.mapping.id", "id")\
        .getOrCreate()

    print("Spark session created successfully.")

    # Read data from HDFS CSV
    # Infer schema can be slow; define it for production
    df = spark.read.csv(HDFS_INPUT_PATH, header=True, inferSchema=True)
    print(f"Read {df.count()} records from CSV.")

    # Perform Batch Analysis (Example: Average delay per airline)
    # Ensure numeric types are correct
    df = df.withColumn("ArrDelayMinutes", col("ArrDelayMinutes").cast("double")) \
           .withColumn("DepDelayMinutes", col("DepDelayMinutes").cast("double"))

    airline_delays = df.groupBy("Reporting_Airline", "OriginCityName", "DestCityName") \
        .agg(
            avg("ArrDelayMinutes").alias("avg_arrival_delay"),
            avg("DepDelayMinutes").alias("avg_departure_delay"),
            count("*").alias("total_flights")
        ) \
        .filter(col("Reporting_Airline").isNotNull()) # Filter out potential null keys

    # Add an ID column for Elasticsearch (optional but good practice)
    # Create a unique enough ID based on the group keys
    airline_delays = airline_delays.withColumn("id",
         expr("concat(Reporting_Airline, '-', OriginCityName, '-', DestCityName)"))

    print("Calculated average delays per airline/origin/destination.")
    airline_delays.show(5, truncate=False) # Show sample results

    # Write results to Elasticsearch
    print(f"Writing results to Elasticsearch index: {ES_INDEX}...")
    airline_delays.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", f"{ES_INDEX}") \
        .option("es.nodes", ES_HOST) \
        .option("es.port", ES_PORT) \
        .option("es.mapping.id", "id") \
        .mode("overwrite") \
        .save()

    print("Batch processing finished successfully.")

except Exception as e:
    print(f"An error occurred during batch processing: {e}")
    import traceback
    traceback.print_exc()

finally:
    if 'spark' in locals():
        spark.stop()
        print("Spark session stopped.")