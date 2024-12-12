from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, TimestampType

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaToSpark") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.kafka:kafka-clients:3.3.2") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.executor.extraLibraryPath", "$HADOOP_HOME/bin") \
    .config("spark.driver.extraLibraryPath", "$HADOOP_HOME/bin") \
    .getOrCreate()

# Kafka configurations
kafka_bootstrap_servers = "broker:29092"  # Địa chỉ Kafka Broker
kafka_topic = "test"  # Tên topic cần đọc

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Chọn key và value từ topic
kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Định nghĩa schema nếu dữ liệu là JSON
schema = StructType([
    StructField("battle_time", StringType(), True),
    StructField("game_mode", IntegerType(), True),
    StructField("player1", StructType([
        StructField("tag", StringType(), True),
        StructField("trophies", IntegerType(), True),
        StructField("crowns", IntegerType(), True),
        StructField("card1", IntegerType(), True),
        StructField("card2", IntegerType(), True),
        StructField("card3", IntegerType(), True),
        StructField("card4", IntegerType(), True),
        StructField("card5", IntegerType(), True),
        StructField("card6", IntegerType(), True),
        StructField("card7", IntegerType(), True),
        StructField("card8", IntegerType(), True)
    ]), True),
    StructField("player2", StructType([
        StructField("tag", StringType(), True),
        StructField("trophies", IntegerType(), True),
        StructField("crowns", IntegerType(), True),
        StructField("card1", IntegerType(), True),
        StructField("card2", IntegerType(), True),
        StructField("card3", IntegerType(), True),
        StructField("card4", IntegerType(), True),
        StructField("card5", IntegerType(), True),
        StructField("card6", IntegerType(), True),
        StructField("card7", IntegerType(), True),
        StructField("card8", IntegerType(), True)
    ]), True)
])


# Chuyển đổi dữ liệu JSON trong value thành DataFrame
parsed_df = kafka_df.withColumn("parsed_value", from_json(col("value"), schema)).select("key", "parsed_value.*")
# parsed_df = kafka_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Xử lý dữ liệu và in ra console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("path", "/tmp/output") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# Chạy stream cho đến khi dừng lại
query.awaitTermination()
