from pyspark.sql import SparkSession

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaToSpark") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,"
            "org.apache.kafka:kafka-clients:2.8.1,"
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.3.3,"
            "org.slf4j:slf4j-api:1.7.36,"
            "org.slf4j:slf4j-log4j12:1.7.36") \
    .config("spark.local.dir", "./tmp") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "big_data_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Dữ liệu Kafka trả về trong cột `value` dạng byte
messages_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# In ra dữ liệu nhận được (chỉ để debug)
query = messages_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "./output") \
    .option("checkpointLocation", "./tmp/spark_checkpoint") \
    .start()

query.awaitTermination()
