import sys, json, hdfs, findspark, os
from pathlib import Path
import signal
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from datetime import datetime
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Khởi tạo Spark và các thư viện cần thiết
findspark.init()

# Kafka Configuration
KAFKA_TOPIC_NAME = "big-data-topic"
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"

# Nạp các biến môi trường từ file .env
load_dotenv()  # Nạp từ file .env trong thư mục hiện tại

# Truy cập các biến môi trường
# INFLUXDB_URL = "http://localhost:8086"  # URL của InfluxDB
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_DB = os.getenv("INFLUXDB_DB")
INFLUXDB_PLAYER_BUCKET = os.getenv("INFLUXDB_PLAYER_BUCKET")
INFLUXDB_CARD_BUCKET = os.getenv("INFLUXDB_CARD_BUCKET")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUXDB_MEASUREMENT = os.getenv("INFLUXDB_MEASUREMENT")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")

# In các giá trị ra để kiểm tra
print(f"INFLUXDB_URL: {INFLUXDB_URL}")
print(f"INFLUXDB_DB: {INFLUXDB_DB}")
print(f"INFLUXDB_PLAYER_BUCKET: {INFLUXDB_PLAYER_BUCKET}")
print(f"INFLUXDB_CARD_BUCKET: {INFLUXDB_CARD_BUCKET}")
print(f"INFLUX_ORG: {INFLUX_ORG}")
print(f"INFLUXDB_MEASUREMENT: {INFLUXDB_MEASUREMENT}")
print(f"INFLUX_TOKEN: {INFLUX_TOKEN}")

# Khởi tạo client InfluxDB
influxdb_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)

def create_bucket_if_not_exists(bucket_name, org):
    try:
        buckets_api = influxdb_client.buckets_api()
        
        bucket = buckets_api.find_bucket_by_name(bucket_name)
        if bucket is None:
            retention_rules = None  

            buckets_api.create_bucket(
                bucket_name=bucket_name,
                org_id=buckets_api.find_organization(org).id, 
                retention_rules=retention_rules,
            )
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        print(f"Error creating bucket: {e}")


# Định nghĩa schema cho dữ liệu đầu vào
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

# Hàm dừng query khi nhận tín hiệu dừng
def stop_query(sig, frame):
    print("Stopping query...")
    query.stop()
    print("Query stopped.")
    spark.stop()
    print("Spark stopped.")
    influxdb_client.close()
    print("InfluxDB client closed.")
    exit(0)

signal.signal(signal.SIGINT, stop_query)
signal.signal(signal.SIGTERM, stop_query)

if __name__ == "__main__":
    # Khởi tạo Spark session
    spark = (
        SparkSession.builder.appName("KafkaInfluxDBStreaming")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Đọc dữ liệu từ Kafka
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Phân tích thông điệp Kafka
    data_stream = kafka_stream.select(col("value").cast("string").alias("data"))
    structured_stream = data_stream.select(from_json(col("data"), schema).alias("game_result"))

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



    # Hàm xử lý từng batch
    def process_batch(batch_df, batch_id):
        print(f"Processing batch {batch_id} started!")
        expanded_df = batch_df.select("game_result.*")
        print(expanded_df.show())
        for row in expanded_df.collect():
            try:
                player1 = row['player1']
                player2 = row['player2']
                merged_deck = set(player1['deck']) | set(player2['deck'])
                for card in merged_deck:
                    print(f"Card: {card}")
                    crowns = 0
                    trophies = 0
                    if card in player1['deck']:
                        crowns += player1['crowns']
                        trophies = player1['trophies']
                    if card in player2['deck']:
                        crowns -= player2['crowns']
                        trophies = player2['trophies']
                    if card in player1['deck'] and card in player2['deck']:
                        trophies = (player1['trophies'] + player2['trophies'])//2
                    cardPoint = (
                        Point("card_results")
                        .tag("battle_time", row['battle_time'])
                        .field("game_mode", row['game_mode'])
                        .tag("card", card)
                        .field("crowns", crowns)
                        .field("trophies", int(trophies))
                    )
                    write_api.write(bucket=INFLUXDB_CARD_BUCKET, org=INFLUX_ORG, record=cardPoint)
                point = (
                    Point("game_results")
                    .tag("battle_time", row['battle_time'])
                    .field("game_mode", row['game_mode'])
                    .tag("player1_tag", player1['tag'])
                    .field("player1_trophies", player1['trophies'])
                    .field("player1_crowns", player1['crowns'])
                    .tag("player2_tag", player2['tag'])
                    .field("player2_trophies", player2['trophies'])
                    .field("player2_crowns", player2['crowns'])
                )

                write_api.write(bucket=INFLUXDB_PLAYER_BUCKET, org=INFLUX_ORG, record=point)
            except Exception as e:
                print(f"Error writing to InfluxDB: {e}")

        
        print ("Batch processing done!")
        print(f"Number of rows: {expanded_df.count()}")
        print(f"Batch {batch_id} processed successfully!")

    # Bắt đầu streaming query
    query = (
        structured_stream.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()
