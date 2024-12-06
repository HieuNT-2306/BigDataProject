from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField
from pymongo import MongoClient

# MongoDB configuration
MONGO_URL = "mongodb://root:admin@mongodb:27017"
DATABASE = "big_data"
COLLECTION = "data"

# Kafka configuration
KAFKA_BROKER = "broker:29092"
TOPIC = "big_data_topic"

# Define MongoDB writer function
def write_to_mongodb(row):
    client = MongoClient(MONGO_URL)
    db = client[DATABASE]
    collection = db[COLLECTION]
    collection.insert_one(row.asDict())
    client.close()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaToMongoDB") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Define Kafka schema
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .load()

# Parse Kafka messages
messages_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
parsed_df = messages_df.withColumn("json_data", from_json(col("value"), schema))

# Process and send to MongoDB
parsed_df.writeStream \
    .foreach(write_to_mongodb) \
    .start() \
    .awaitTermination()
