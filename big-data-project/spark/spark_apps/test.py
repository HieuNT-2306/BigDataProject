import sys, json, hdfs, findspark, os
from pathlib import Path

path_to_utils = Path(__file__).parent.parent
sys.path.insert(0, str(path_to_utils))
sys.path.append("/app")

from confluent_kafka import Consumer, KafkaError
# from script.utils import load_environment_variables
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from InfluxDBWriter import InfluxDBWriter
from dotenv import load_dotenv

# load_dotenv()
findspark.init()
# env_vars = load_environment_variables()

KAFKA_TOPIC_NAME = "test"
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"

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

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("KafkaInfluxDBStreaming")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    stockDataframe = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .load()

    stockDataframe = stockDataframe.select(col("value").cast("string").alias("data"))
    inputStream = stockDataframe.selectExpr("CAST(data as STRING)")

    stockDataframe = inputStream.select(from_json(col("data"), schema).alias("game_result"))
    expandedDf = stockDataframe.select("game_result.*")
    influxdb_writer = InfluxDBWriter('primary', 'game-results-v1')
    print("InfluxDB_Init Done")

    def process_batch(batch_df, batch_id):
        gameResults = batch_df.select("game_result.*")
        for gameResult in gameResults.collect():
            battle_time = gameResult["battle_time"]
            game_mode = gameResult["game_mode"]
            player1 = gameResult["player1"]
            player2 = gameResult["player2"]

            tags = {
                "player1_tag": player1["tag"],
                "player2_tag": player2["tag"],
                "game_mode": game_mode
            }
            fields = {
                "player1_trophies": player1['trophies'],
                "player1_crowns": player1['crowns'],
                "player2_trophies": player2['trophies'],
                "player2_crowns": player2['crowns'],
            }
            influxdb_writer.process(battle_time, tags, fields)

            row_dict = gameResult.asDict()
            row_dict['battle_time'] = row_dict['battle_time']  
            json_string = json.dumps(row_dict)
            print(json_string)
            print("----------------------")
            hdfs.write_to_hdfs(json_string)
        print(f"Batch processed {batch_id} done!")

    query = stockDataframe \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()