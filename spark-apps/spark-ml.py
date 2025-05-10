#!/usr/bin/env python3
from pyspark.ml.classification import LinearSVC # RandomForestClassifier (có thể dùng nếu muốn)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col # month, dayofweek, dayofmonth (đã có trong Parquet)
from pyspark.ml.feature import StandardScaler, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.pipeline import Pipeline

# --- Cấu hình ---
HDFS_NAMENODE = "hdfs://host.docker.internal:9000" # Hoặc "hdfs://192.168.1.155:9000"
PROCESSED_DATA_PATH_PARQUET = f"{HDFS_NAMENODE}/bigdata/processed_data/historical_flights.parquet"

# Đường dẫn lưu trữ pipeline model trên HDFS
DELAY_PIPELINE_OUTPUT_PATH = f"{HDFS_NAMENODE}/bigdata/models/delay_prediction_pipeline"
CANCELLATION_PIPELINE_OUTPUT_PATH = f"{HDFS_NAMENODE}/bigdata/models/cancellation_prediction_pipeline"

ELASTICSEARCH_NODES = "http://elasticsearch:9200" # Giữ lại nếu sau này muốn ghi kết quả batch prediction

def main():
    spark = SparkSession \
        .builder \
        .appName("Flight ML Model Training") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-35_2.12:8.14.1") \
        .config("spark.elasticsearch.nodes", ELASTICSEARCH_NODES) \
        .config("spark.elasticsearch.port", "9200") \
        .config("spark.elasticsearch.nodes.wan.only", "false") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .getOrCreate()

    print(f"Đang đọc dữ liệu đã tiền xử lý từ Parquet: {PROCESSED_DATA_PATH_PARQUET}")
    df_all_data = spark.read.parquet(PROCESSED_DATA_PATH_PARQUET)
    # df_all_data.printSchema()
    # print(f"Số dòng đọc được từ Parquet cho ML: {df_all_data.count()}")

    """
    ========================================================================================
    1. Delay Prediction Model
    ========================================================================================
    """
    print("\n--- Bắt đầu Huấn luyện Mô hình Dự đoán Độ trễ ---")
    # Chọn các cột cần thiết và lọc dữ liệu cho mô hình dự đoán độ trễ
    # Cột YEAR, MONTH, DAYOFMONTH, DAYOFWEEK đã có sẵn từ file Parquet
    df_delay_input = df_all_data.select("MONTH", "DAYOFMONTH", "DAYOFWEEK",
                                     "OP_CARRIER", "ORIGIN", "DEST",
                                     "ARR_DELAY", "DISTANCE", "IS_DELAY") \
                                .filter(col("ARR_DELAY").isNotNull()) \
                                .filter(col("DISTANCE").isNotNull()) \
                                .filter(col("OP_CARRIER").isNotNull()) \
                                .filter(col("ORIGIN").isNotNull()) \
                                .filter(col("DEST").isNotNull()) \
                                .sample(fraction=0.01, seed=42) # Lấy mẫu để huấn luyện nhanh hơn, điều chỉnh fraction nếu cần

    print(f"Số dòng sử dụng cho mô hình dự đoán độ trễ (sau khi lọc và lấy mẫu): {df_delay_input.count()}")
    if df_delay_input.count() == 0:
        print("Không có dữ liệu để huấn luyện mô hình dự đoán độ trễ. Kết thúc.")
        # spark.stop() # Không cần dừng cả session nếu còn mô hình khác
    else:
        # Preprocessing Stages
        indexer_delay = StringIndexer(
            inputCols=["OP_CARRIER", "ORIGIN", "DEST"],
            outputCols=["INDEX_CARRIER", "INDEX_ORIGIN", "INDEX_DEST"],
            handleInvalid="keep"
        )
        oneHotEncoder_delay = OneHotEncoder(
            inputCols=["INDEX_CARRIER", "INDEX_ORIGIN", "INDEX_DEST"],
            outputCols=["ONEHOT_CARRIER", "ONEHOT_ORIGIN", "ONEHOT_DEST"]
        )
        assembler_delay = VectorAssembler(
            inputCols=["MONTH", "DAYOFMONTH", "DAYOFWEEK",
                       "ONEHOT_CARRIER", "ONEHOT_ORIGIN", "ONEHOT_DEST", "DISTANCE"],
            outputCol="FEATURES_UNSCALED"
        )
        scaler_delay = StandardScaler(
            inputCol="FEATURES_UNSCALED", outputCol="FEATURES",
            withStd=True, withMean=True # Hoặc withMean=False nếu LinearSVC xử lý tốt hơn
        )

        # Model
        lsvc_delay = LinearSVC(maxIter=10, regParam=0.1, labelCol="IS_DELAY", featuresCol="FEATURES")

        # Pipeline
        pipeline_delay = Pipeline(stages=[indexer_delay, oneHotEncoder_delay, assembler_delay, scaler_delay, lsvc_delay])

        # Split data
        train_data_delay, test_data_delay = df_delay_input.randomSplit([0.8, 0.2], seed=1234)
        # train_data_delay.cache()
        # test_data_delay.cache()

        print("Đang huấn luyện pipeline dự đoán độ trễ...")
        pipeline_model_delay = pipeline_delay.fit(train_data_delay)
        print("Huấn luyện pipeline dự đoán độ trễ hoàn tất.")

        # Evaluate
        predictions_delay = pipeline_model_delay.transform(test_data_delay)
        evaluator_accuracy_delay = MulticlassClassificationEvaluator(metricName="accuracy", labelCol="IS_DELAY", predictionCol="prediction")
        accuracy_delay = evaluator_accuracy_delay.evaluate(predictions_delay)
        print(f"Mô hình Dự đoán Độ trễ - Test set accuracy = {accuracy_delay}")

        # Thêm các metric khác nếu cần (F1, precision, recall)
        evaluator_f1_delay = MulticlassClassificationEvaluator(metricName="f1", labelCol="IS_DELAY", predictionCol="prediction")
        f1_delay = evaluator_f1_delay.evaluate(predictions_delay)
        print(f"Mô hình Dự đoán Độ trễ - Test set F1 score = {f1_delay}")

        predictions_delay.select("prediction", "IS_DELAY", "probability" if "probability" in predictions_delay.columns else "rawPrediction").show(20, truncate=False)

        # Save pipeline model
        print(f"Đang lưu pipeline dự đoán độ trễ vào: {DELAY_PIPELINE_OUTPUT_PATH}")
        pipeline_model_delay.write().overwrite().save(DELAY_PIPELINE_OUTPUT_PATH)
        print("Lưu pipeline dự đoán độ trễ hoàn tất.")

        train_data_delay.unpersist()
        test_data_delay.unpersist()

    """
    ========================================================================================
    2. Cancellation Prediction Model
    ========================================================================================
    """
    print("\n--- Bắt đầu Huấn luyện Mô hình Dự đoán Hủy chuyến ---")
    # Chọn các cột cần thiết và lọc dữ liệu cho mô hình dự đoán hủy chuyến
    df_cancellation_input = df_all_data.select("MONTH", "DAYOFMONTH", "DAYOFWEEK",
                                             "OP_CARRIER", "ORIGIN", "DEST",
                                             "DISTANCE", "CANCELLED") \
                                       .filter(col("CANCELLED").isNotNull()) \
                                       .filter(col("DISTANCE").isNotNull()) \
                                       .filter(col("OP_CARRIER").isNotNull()) \
                                       .filter(col("ORIGIN").isNotNull()) \
                                       .filter(col("DEST").isNotNull()) \
                                       .sample(fraction=0.2, seed=123) # Lấy mẫu

    print(f"Số dòng sử dụng cho mô hình dự đoán hủy chuyến (sau khi lọc và lấy mẫu): {df_cancellation_input.count()}")

    if df_cancellation_input.count() == 0 or df_cancellation_input.select("CANCELLED").distinct().count() < 2:
        print("Không có đủ dữ liệu hoặc không đủ lớp (0 và 1) để huấn luyện mô hình dự đoán hủy chuyến. Kết thúc.")
    else:
        # Preprocessing Stages (tương tự như delay, nhưng labelCol là "CANCELLED")
        indexer_canc = StringIndexer(
            inputCols=["OP_CARRIER", "ORIGIN", "DEST"],
            outputCols=["INDEX_CARRIER", "INDEX_ORIGIN", "INDEX_DEST"],
            handleInvalid="keep"
        )
        oneHotEncoder_canc = OneHotEncoder(
            inputCols=["INDEX_CARRIER", "INDEX_ORIGIN", "INDEX_DEST"],
            outputCols=["ONEHOT_CARRIER", "ONEHOT_ORIGIN", "ONEHOT_DEST"]
        )
        assembler_canc = VectorAssembler(
            inputCols=["MONTH", "DAYOFMONTH", "DAYOFWEEK",
                       "ONEHOT_CARRIER", "ONEHOT_ORIGIN", "ONEHOT_DEST", "DISTANCE"],
            outputCol="FEATURES_UNSCALED"
        )
        scaler_canc = StandardScaler(
            inputCol="FEATURES_UNSCALED", outputCol="FEATURES",
            withStd=True, withMean=True
        )

        # Model
        lsvc_canc = LinearSVC(maxIter=10, regParam=0.1, labelCol="CANCELLED", featuresCol="FEATURES")

        # Pipeline
        pipeline_canc = Pipeline(stages=[indexer_canc, oneHotEncoder_canc, assembler_canc, scaler_canc, lsvc_canc])

        # Split data
        train_data_canc, test_data_canc = df_cancellation_input.randomSplit([0.8, 0.2], seed=5678)
        # train_data_canc.cache()
        # test_data_canc.cache()

        print("Đang huấn luyện pipeline dự đoán hủy chuyến...")
        pipeline_model_canc = pipeline_canc.fit(train_data_canc)
        print("Huấn luyện pipeline dự đoán hủy chuyến hoàn tất.")

        # Evaluate
        predictions_canc = pipeline_model_canc.transform(test_data_canc)
        evaluator_accuracy_canc = MulticlassClassificationEvaluator(metricName="accuracy", labelCol="CANCELLED", predictionCol="prediction")
        accuracy_canc = evaluator_accuracy_canc.evaluate(predictions_canc)
        print(f"Mô hình Dự đoán Hủy chuyến - Test set accuracy = {accuracy_canc}")

        evaluator_f1_canc = MulticlassClassificationEvaluator(metricName="f1", labelCol="CANCELLED", predictionCol="prediction")
        f1_canc = evaluator_f1_canc.evaluate(predictions_canc)
        print(f"Mô hình Dự đoán Hủy chuyến - Test set F1 score = {f1_canc}")

        predictions_canc.select("prediction", "CANCELLED", "probability" if "probability" in predictions_canc.columns else "rawPrediction").show(20, truncate=False)

        # Save pipeline model
        print(f"Đang lưu pipeline dự đoán hủy chuyến vào: {CANCELLATION_PIPELINE_OUTPUT_PATH}")
        pipeline_model_canc.write().overwrite().save(CANCELLATION_PIPELINE_OUTPUT_PATH)
        print("Lưu pipeline dự đoán hủy chuyến hoàn tất.")

        train_data_canc.unpersist()
        test_data_canc.unpersist()

    print("\n--- Hoàn tất quá trình huấn luyện ML ---")
    spark.stop()

if __name__ == "__main__":
    main()