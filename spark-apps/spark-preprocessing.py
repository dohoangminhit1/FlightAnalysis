#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, to_date, when, lit
from pyspark.sql.types import IntegerType, FloatType, StringType, DateType, DoubleType

# --- Cấu hình ---
# Đường dẫn HDFS được lấy từ cấu hình của bạn (core-site.xml: fs.defaultFS)
HDFS_NAMENODE = "hdfs://host.docker.internal:9000" # Hoặc "hdfs://192.168.1.155:9000" nếu host.docker.internal không hoạt động như mong đợi
HDFS_INPUT_PATH = f"{HDFS_NAMENODE}/bigdata/raw_data/2023_*.csv"
HDFS_OUTPUT_PATH_PARQUET = f"{HDFS_NAMENODE}/bigdata/processed_data/historical_flights.parquet"
# HDFS_OUTPUT_PATH_CSV = f"{HDFS_NAMENODE}/bigdata/processed_data/historical_flights.csv"

ELASTICSEARCH_NODES = "http://elasticsearch:9200" # Từ docker-compose.yml

def main():
    spark = SparkSession \
        .builder \
        .appName("Flight Batch Preprocessing") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-35_2.12:8.14.1") \
        .config("spark.elasticsearch.nodes", ELASTICSEARCH_NODES) \
        .config("spark.elasticsearch.port", "9200") \
        .config("spark.elasticsearch.nodes.wan.only", "false") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")\
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")\
        .getOrCreate()

    # Ghi đè cấu hình HDFS mà Spark có thể đọc từ classpath (nếu hadoop-conf được mount đúng)
    # Nếu file core-site.xml và hdfs-site.xml được mount vào thư mục hadoop-conf
    # và thư mục đó nằm trong classpath của Spark, Spark sẽ tự động đọc chúng.
    # Việc đặt 'spark.hadoop.fs.defaultFS' ở đây sẽ ghi đè giá trị từ file config.
    # Tuy nhiên, để rõ ràng, chúng ta sử dụng HDFS_NAMENODE đã định nghĩa.

    print(f"Spark Session được cấu hình để kết nối HDFS tại: {spark.conf.get('spark.hadoop.fs.defaultFS')}")
    print(f"Đang đọc dữ liệu từ: {HDFS_INPUT_PATH}")

    # Kiểm tra kết nối HDFS cơ bản (tùy chọn)
    try:
        print("Danh sách file/thư mục trong /bigdata/raw_data (kiểm tra kết nối HDFS):")
        raw_data_listing = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration()).listStatus(
            spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"{HDFS_NAMENODE}/bigdata/raw_data")
        )
        for item in raw_data_listing:
            print(item.getPath())
    except Exception as e:
        print(f"Lỗi khi liệt kê file trên HDFS: {e}")
        print("Vui lòng kiểm tra cấu hình HDFS và kết nối mạng giữa Spark container và HDFS NameNode.")
        # spark.stop() # Có thể dừng ở đây nếu không kết nối được
        # return

    df = spark.read.option("header", True).csv(HDFS_INPUT_PATH)

    df.printSchema()
    print(f"Số dòng đọc được ban đầu: {df.count()}")

    # --- Lựa chọn và Đổi tên Cột ---
    df_selected = df.select(
        to_date(col("FlightDate"), "yyyy-MM-dd").alias("FL_DATE"),
        col("IATA_CODE_Reporting_Airline").alias("OP_CARRIER"),
        col("Flight_Number_Reporting_Airline").alias("OP_CARRIER_FL_NUM"),
        col("Origin").alias("ORIGIN"),
        col("OriginCityName"),
        col("OriginStateName"),
        col("Dest").alias("DEST"),
        col("DestCityName"),
        col("DestStateName"),
        col("CRSDepTime"),
        col("DepTime"),
        col("DepDelay").cast(FloatType()).alias("DEP_DELAY"),
        col("DepDelayMinutes").cast(FloatType()),
        col("DepDel15").cast(FloatType()),
        col("TaxiOut").cast(FloatType()).alias("TAXI_OUT"),
        col("WheelsOff"),
        col("WheelsOn"),
        col("TaxiIn").cast(FloatType()).alias("TAXI_IN"),
        col("CRSArrTime"),
        col("ArrTime"),
        col("ArrDelay").cast(FloatType()).alias("ARR_DELAY"),
        col("ArrDelayMinutes").cast(FloatType()),
        col("ArrDel15").cast(FloatType()),
        col("Cancelled").cast(DoubleType()).cast(IntegerType()).alias("CANCELLED"),
        col("CancellationCode").alias("CANCELLATION_CODE"),
        col("Diverted").cast(DoubleType()).cast(IntegerType()).alias("DIVERTED"),
        col("CRSElapsedTime").cast(FloatType()),
        col("ActualElapsedTime").cast(FloatType()).alias("ACTUAL_ELAPSED_TIME"),
        col("AirTime").cast(FloatType()).alias("AIR_TIME"),
        col("Distance").cast(FloatType()).alias("DISTANCE"),
        col("CarrierDelay").cast(FloatType()).alias("CARRIER_DELAY"),
        col("WeatherDelay").cast(FloatType()).alias("WEATHER_DELAY"),
        col("NASDelay").cast(FloatType()).alias("NAS_DELAY"),
        col("SecurityDelay").cast(FloatType()).alias("SECURITY_DELAY"),
        col("LateAircraftDelay").cast(FloatType()).alias("LATE_AIRCRAFT_DELAY")
    )

    # --- Tạo các cột dẫn xuất ---
    df_transformed = df_selected \
        .withColumn("YEAR", year(col("FL_DATE"))) \
        .withColumn("MONTH", month(col("FL_DATE"))) \
        .withColumn("DAYOFMONTH", dayofmonth(col("FL_DATE"))) \
        .withColumn("DAYOFWEEK", dayofweek(col("FL_DATE"))) \
        .withColumn("IS_DELAY", (col("ARR_DELAY") > 0).cast("int"))

    # --- Xử lý NULL/NA ---
    df_filtered = df_transformed.filter(col("FL_DATE").isNotNull())

    # --- Loại bỏ trùng lặp ---
    df_deduplicated = df_filtered.dropDuplicates()

    print("Schema sau khi tiền xử lý:")
    df_deduplicated.printSchema()
    print(f"Số dòng sau khi tiền xử lý và loại bỏ trùng lặp: {df_deduplicated.count()}")

    # --- Ghi kết quả ra HDFS (ưu tiên Parquet) ---
    print(f"Đang ghi dữ liệu đã xử lý ra Parquet: {HDFS_OUTPUT_PATH_PARQUET}")
    df_deduplicated.coalesce(5) \
        .write \
        .mode("overwrite") \
        .parquet(HDFS_OUTPUT_PATH_PARQUET)
    print("Ghi Parquet hoàn tất.")

    spark.stop()

if __name__ == "__main__":
    main()