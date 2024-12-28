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

    #id to name
    cards_path = "/opt/spark/apps/cards.json"
    gamemodes_path = "/opt/spark/apps/gamemodes.json"
    
    cardjson = json.load(open(cards_path))
    gamemodesjson = json.load(open(gamemodes_path))
    # print(f"Cards: {cardjson}")
    # print(f"Gamemodes: {gamemodesjson}")    
    # Tạo ánh xạ từ id sang name
    # Ánh xạ từ card id sang tên card
    cards_mapping = {card["id"]: card["name"] for card in cardjson}

    # Ánh xạ từ game mode id sang tên game mode
    gamemodes_mapping = {mode["id"]: mode["name_en"] for mode in gamemodesjson}


    def process_batch(batch_df, batch_id):
        print(f"Batch processing {batch_id} started!")
        gameResults = batch_df.select("game_result.*")
        print(f"{gameResults.count()} records in this batch")

        # Thay thế `game_mode` và `deck` bằng `name`
        def map_game_mode(game_mode_id):
            return gamemodes_mapping.get(game_mode_id, "Unknown")

        def map_deck(deck_ids):
            return [cards_mapping.get(card_id, f"Unknown({card_id})") for card_id in deck_ids]

        # Áp dụng ánh xạ cho từng batch
        mapped_df = batch_df.select(
            col("game_result.battle_time").alias("battle_time"),
            udf(map_game_mode)(col("game_result.game_mode")).alias("game_mode_name"),
            col("game_result.player1.tag").alias("player1_tag"),
            col("game_result.player1.trophies").alias("player1_trophies"),
            col("game_result.player1.crowns").alias("player1_crowns"),
            udf(map_deck)(col("game_result.player1.deck")).alias("player1_deck_names"),
            col("game_result.player2.tag").alias("player2_tag"),
            col("game_result.player2.trophies").alias("player2_trophies"),
            col("game_result.player2.crowns").alias("player2_crowns"),
            udf(map_deck)(col("game_result.player2.deck")).alias("player2_deck_names")
        )
        # current_timestamp = datetime.now().isoformat()
        # # Ghi từng document vào Elasticsearch
        # for gameResult in gameResults.collect():
        #     document = gameResult.asDict(recursive=True)
        #     document['timestamp'] = current_timestamp
        #     response = es.index(index=ES_INDEX, document=document)
        #     print("Inserted document into Elasticsearch:", response)            
        # print(f"Batch processed {batch_id} done!")
        # Ghi từng document vào Elasticsearch
        for row in mapped_df.collect():
            document = row.asDict(recursive=True)
            document['timestamp'] = datetime.now().isoformat()
            response = es.index(index=ES_INDEX, document=document)
            print("Inserted document into Elasticsearch:", response)

        print(f"Batch processed {batch_id} done!")
    query = stockDataframe \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
