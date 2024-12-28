import sys, json, hdfs, findspark, os
from pathlib import Path
import signal

path_to_utils = Path(__file__).parent.parent
sys.path.insert(0, str(path_to_utils))
sys.path.append("/app")

# from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

findspark.init()

KAFKA_TOPIC_NAME = "big-data-topic"
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
load_dotenv()

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "game_results")

schema = StructType([
    StructField("battle_time", StringType(), True),
    StructField("game_mode", IntegerType(), True),
    StructField("player1", StructType([
        StructField("tag", StringType(), True),
        StructField("trophies", IntegerType(), True),
        StructField("crowns", IntegerType(), True),
        StructField("deck", ArrayType(IntegerType()), True),
    ]), True),
    StructField("player2", StructType([
        StructField("tag", StringType(), True),
        StructField("trophies", IntegerType(), True),
        StructField("crowns", IntegerType(), True),
        StructField("deck", ArrayType(IntegerType()), True),
    ]), True)
])

def stop_query(sig, frame):
    print("Stopping query...")
    query.stop()
    print("Query stopped.")
    spark.stop()
    print("Spark stopped.")
    exit(0)

signal.signal(signal.SIGINT, stop_query)
signal.signal(signal.SIGTERM, stop_query)

if __name__ == "__main__":
    # Elasticsearch client
    es = Elasticsearch([ES_HOST])

    spark = (
        SparkSession.builder.appName("KafkaElasticsearchStreaming")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    stockDataframe = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    stockDataframe = stockDataframe.select(col("value").cast("string").alias("data"))
    inputStream = stockDataframe.selectExpr("CAST(data as STRING)")

    stockDataframe = inputStream.select(from_json(col("data"), schema).alias("game_result"))
    expandedDf = stockDataframe.select("game_result.*")
    print("Elasticsearch Init Done - Testing Kafka")

    def process_batch(batch_df, batch_id):
        print(f"Batch processing {batch_id} started!")
        gameResults = batch_df.select("game_result.*")
        print(f"{gameResults.count()} records in this batch")
        current_timestamp = datetime.now().isoformat()
        # Ghi từng document vào Elasticsearch
        for gameResult in gameResults.collect():
            document = gameResult.asDict(recursive=True)
            document['timestamp'] = current_timestamp
            response = es.index(index=ES_INDEX, document=document)
            print("Inserted document into Elasticsearch:", response)            
        print(f"Batch processed {batch_id} done!")

    query = stockDataframe \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
