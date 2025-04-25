from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count
import json
import requests

# 1. Tạo Spark session
spark = SparkSession.builder \
    .appName("Flight Word Cloud to Elasticsearch") \
    .getOrCreate()

# 2. Đọc dữ liệu từ HDFS (toàn bộ các file)
df = spark.read.csv("hdfs://hadoop-namenode:9000/input/*.csv", header=True, inferSchema=True)

# 3. Chọn cột thành phố đến và loại bỏ giá trị null
df_dest = df.select("DestCityName").na.drop()

# 4. Đếm tần suất xuất hiện của từng thành phố đến
dest_count = df_dest.groupBy("DestCityName") \
    .agg(count("*").alias("count")) \
    .orderBy(desc("count"))

# 5. Gửi dữ liệu lên Elasticsearch
es_url = "http://elasticsearch:9200/flight_wordcloud/_doc"
headers = {"Content-Type": "application/json"}

for row in dest_count.limit(100).collect():  # Giới hạn 100 từ nhiều nhất cho đẹp
    doc = {
        "city": row["DestCityName"],
        "count": int(row["count"])
    }
    res = requests.post(es_url, headers=headers, data=json.dumps(doc))
    if res.status_code in (200, 201):
        print(f"✅ Đã gửi {row['DestCityName']} ({row['count']}) vào Elasticsearch.")
    else:
        print(f"❌ Lỗi gửi {row['DestCityName']}: {res.status_code} - {res.text}")

print("🌈 Word cloud data đã sẵn sàng trong Elasticsearch!")
