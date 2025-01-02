# Import necessary libraries for data processing and streaming
import sys
import json
import findspark
import os
from pathlib import Path
import signal
import traceback
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark and load environment variables
findspark.init()
load_dotenv()

# Configuration constants
KAFKA_TOPIC_NAME = "big-data-topic"
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = "BigData"
MONGO_COLLECTION = "game_results"
CARD_RECOMMENDATIONS_COLLECTION = "card_recommendations"
MODEL_COLLECTION = "card_recommendation_models"

# Define model storage location - this should be a mounted volume in Docker
MODEL_BASE_DIR = os.getenv('MODEL_BASE_DIR', '/opt/spark/models')
os.makedirs(MODEL_BASE_DIR, exist_ok=True)

# Define schema for game data
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

class CardRecommender:
    """Training phase class - handles model training and persistence"""
    def __init__(self, spark):
        self.spark = spark
        self.als = ALS(
            maxIter=10,
            regParam=0.01,
            userCol="deck_pattern_id",
            itemCol="next_card",
            ratingCol="effectiveness_score",
            coldStartStrategy="drop"
        )
        self.model = None
        self.card_matrix = None
        self.deck_pattern_mapping = None

    def calculate_trophy_performance_boost(self, player_trophies, opponent_trophies, outcome):
        """Calculate performance boost based on trophy difference and game outcome."""
        trophy_diff = opponent_trophies - player_trophies
        trophy_ratio = trophy_diff / F.greatest(player_trophies, F.lit(100))
        
        return F.when(
            outcome.eqNullSafe("win"),
            1.0 + F.greatest(F.lit(0), trophy_ratio * 0.3)
        ).when(
            outcome.eqNullSafe("draw"),
            0.5 + F.greatest(F.lit(0), trophy_ratio * 0.2)
        ).otherwise(
            F.greatest(F.lit(0.1), 1.0 - F.abs(F.least(F.lit(0), trophy_ratio * 0.2)))
        )

    def generate_card_sequences(self, deck, performance_score):
        """Generate training sequences from a deck."""
        sequences = []
        for i in range(len(deck)-1):
            current_sequence = deck[:i+1]
            next_card = deck[i+1]
            sequences.append({
                'deck_pattern': ','.join(map(str, current_sequence)),
                'next_card': next_card,
                'sequence_length': i+1,
                'performance_score': performance_score
            })
        return sequences

    def prepare_training_data(self, game_results_df):
        """Transform game data into training data for sequential deck building."""
        try:
            # Extract and flatten nested columns
            deck_data = game_results_df.select(
                F.col("battle_time"),
                F.col("game_mode"),
                F.col("player1.crowns").alias("p1_crowns"),
                F.col("player2.crowns").alias("p2_crowns"),
                F.col("player1.trophies").alias("p1_trophies"),
                F.col("player2.trophies").alias("p2_trophies"),
                F.col("player1.deck").alias("player1_deck"),
                F.col("player2.deck").alias("player2_deck")
            )
            
            # Determine game outcomes
            deck_data = deck_data.withColumn(
                "game_outcome",
                F.when(
                    F.col("p1_crowns") > F.col("p2_crowns"), 
                    F.lit("win")
                ).when(
                    F.col("p1_crowns") < F.col("p2_crowns"), 
                    F.lit("loss")
                ).otherwise(F.lit("draw"))
            )

            # Process player 1's deck data
            player1_sequences = deck_data.select(
                F.col("player1_deck"),
                F.col("p1_trophies"),
                F.col("p2_trophies"),
                F.col("game_outcome"),
                self.calculate_trophy_performance_boost(
                    F.col("p1_trophies"),
                    F.col("p2_trophies"),
                    F.col("game_outcome")
                ).alias("performance_score")
            )

            # Process player 2's deck data with inverted outcomes
            player2_sequences = deck_data.select(
                F.col("player2_deck"),
                F.col("p2_trophies"),
                F.col("p1_trophies"),
                F.when(
                    F.col("game_outcome").eqNullSafe("win"),
                    F.lit("loss")
                ).when(
                    F.col("game_outcome").eqNullSafe("loss"),
                    F.lit("win")
                ).otherwise(
                    F.col("game_outcome")
                ).alias("game_outcome"),
                self.calculate_trophy_performance_boost(
                    F.col("p2_trophies"),
                    F.col("p1_trophies"),
                    F.col("game_outcome")
                ).alias("performance_score")
            )

            # Generate card sequences
            sequences = []
            for row in player1_sequences.collect():
                sequences.extend(self.generate_card_sequences(row.player1_deck, row.performance_score))
            for row in player2_sequences.collect():
                sequences.extend(self.generate_card_sequences(row.player2_deck, row.performance_score))

            # Convert sequences to DataFrame
            all_sequences = self.spark.createDataFrame(sequences)

            # Create mapping of deck patterns to numeric IDs
            deck_patterns = all_sequences.select("deck_pattern").distinct()
            deck_pattern_mapping = deck_patterns.withColumn(
                "deck_pattern_id", 
                F.monotonically_increasing_id()
            )

            # Join with mapping to get numeric IDs
            all_sequences = all_sequences.join(
                deck_pattern_mapping,
                "deck_pattern"
            )

            # Calculate effectiveness metrics using numeric IDs
            window_spec = Window.partitionBy("deck_pattern_id")
            sequence_matrix = all_sequences.groupBy("deck_pattern_id", "next_card").agg(
                F.count("*").alias("total_appearances"),
                F.avg("performance_score").alias("weighted_performance"),
                F.stddev("performance_score").alias("performance_consistency"),
                F.avg("sequence_length").alias("avg_sequence_length")
            )

            # Calculate final effectiveness scores
            training_data = sequence_matrix.withColumn(
                "frequency_score",
                F.col("total_appearances") / F.sum("total_appearances").over(window_spec)
            ).withColumn(
                "consistency_factor",
                1 / (1 + F.coalesce(F.col("performance_consistency"), F.lit(1)))
            ).withColumn(
                "effectiveness_score",
                (F.col("frequency_score") * 0.20 +
                 F.col("weighted_performance") * 0.45 +
                 F.col("consistency_factor") * 0.35)
            )
            
            # Store both training data and mapping
            self.card_matrix = training_data
            self.deck_pattern_mapping = deck_pattern_mapping
            return training_data

        except Exception as e:
            print(f"Error in prepare_training_data: {str(e)}")
            traceback.print_exc()
            raise

    def train(self, training_data):
        """Train the recommendation model."""
        self.model = self.als.fit(training_data)

    def save_model_to_mongodb(self, mongo_client):
        """Save the trained model and associated data using persistent storage."""
        try:
            db = mongo_client[MONGO_DATABASE]
            model_collection = db[MODEL_COLLECTION]
            
            # Generate unique model name and ensure directory exists
            model_name = f"als_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            model_dir = os.path.join(MODEL_BASE_DIR, model_name)
            os.makedirs(model_dir, exist_ok=True)
            
            # Save the model to persistent storage
            self.model.write().overwrite().save(model_dir)
            
            # Store metadata in MongoDB
            model_data = {
                'timestamp': datetime.now(),
                'model_name': model_name,
                'model_path': model_dir,
                'deck_pattern_mapping': self.deck_pattern_mapping.toPandas().to_dict('records'),
                'card_matrix': self.card_matrix.toPandas().to_dict('records')
            }
            
            # Store in MongoDB
            model_collection.insert_one(model_data)
            print(f"Model successfully saved to {model_dir}")
            
        except Exception as e:
            print(f"Error saving model: {str(e)}")
            traceback.print_exc()
            raise

class CardRecommendationService:
    """Service class for making recommendations using saved model"""
    def __init__(self, spark_session, mongo_uri):
        self.spark = spark_session
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[MONGO_DATABASE]
        self.model = None
        self.deck_pattern_mapping = None
        self.card_matrix = None
        self.load_latest_model()

    def load_latest_model(self):
        """Load the most recent model from persistent storage."""
        try:
            # Get latest model metadata from MongoDB
            model_data = self.db[MODEL_COLLECTION].find_one(
                sort=[('timestamp', -1)]
            )
            
            if not model_data:
                raise ValueError("No trained model found in database")

            # Get model name and verify files exist
            model_name = model_data['model_name']
            model_dir = os.path.join(MODEL_BASE_DIR, model_name)
            
            if not os.path.exists(model_dir):
                raise FileNotFoundError(f"Model directory not found: {model_dir}")
            
            # Load model and mappings
            print(f"Loading model from {model_dir}")
            self.model = ALS.load(model_dir)
            
            # Convert stored mappings back to Spark DataFrames
            mapping_pdf = pd.DataFrame(model_data['deck_pattern_mapping'])
            self.deck_pattern_mapping = self.spark.createDataFrame(mapping_pdf)
            
            matrix_pdf = pd.DataFrame(model_data['card_matrix'])
            self.card_matrix = self.spark.createDataFrame(matrix_pdf)
            
            print("Model and mappings successfully loaded")
            
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            traceback.print_exc()
            raise

    def get_recommendations(self, partial_deck, num_recommendations=3):
        """Get card recommendations for a partial deck."""
        if not self.model or not self.deck_pattern_mapping:
            raise ValueError("Model not loaded")
            
        try:
            # Convert deck to pattern string
            deck_pattern = ','.join(map(str, partial_deck))
            
            # Create DataFrame with the pattern
            pattern_df = self.spark.createDataFrame(
                [[deck_pattern]], 
                ["deck_pattern"]
            )
            
            # Join with mapping to get numeric ID
            pattern_with_id = pattern_df.join(
                self.deck_pattern_mapping,
                "deck_pattern",
                "left"
            )
            
            # Generate recommendations
            recommendations = self.model.recommendForUserSubset(
                pattern_with_id,
                num_recommendations
            )
            
            # Format recommendations
            if recommendations.count() > 0:
                recs = recommendations.collect()[0].recommendations
                return [{
                    'card_id': int(rec.next_card),
                    'score': float(rec.rating)
                } for rec in recs]
            return []
            
        except Exception as e:
            print(f"Error generating recommendations: {str(e)}")
            traceback.print_exc()
            raise

def process_batch(batch_df, batch_id):
    """Process streaming batch data and save trained model."""
    try:
        print(f"Processing batch {batch_id}")
        
        # Set up MongoDB connection
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        game_collection = db[MONGO_COLLECTION]

        # Initialize recommender if needed
        global card_recommender
        if 'card_recommender' not in globals():
            card_recommender = CardRecommender(spark)
            print("Initialized new CardRecommender")

        # Process batch and update model
        if batch_df.count() > 0:
            print(f"Processing {batch_df.count()} records")
            try:
                training_data = card_recommender.prepare_training_data(batch_df)
                if training_data.count() > 0:
                    card_recommender.train(training_data)
                    card_recommender.save_model_to_mongodb(client)
                    print(f"Updated and saved card recommendation model")
            except Exception as e:
                print(f"Error updating recommendation model: {str(e)}")
                traceback.print_exc()

        # Store game results
        for game_row in batch_df.collect():
            try:
                document = game_row.asDict(recursive=True)
                game_collection.insert_one(document)
            except Exception as e:
                print(f"Error storing game result: {str(e)}")
                traceback.print_exc()

        print(f"Successfully processed batch {batch_id}")
        
    except Exception as e:
        print(f"Error in batch processing: {str(e)}")
        traceback.print_exc()
    finally:
        if 'client' in locals():
            client.close()

# Signal handlers for graceful shutdown
def stop_query(sig, frame):
    print("Stopping query...")
    if 'query' in globals():
        query.stop()
        print("Query stopped.")
    if 'spark' in globals():
        spark.stop()
        print("Spark stopped.")
    exit(0)

signal.signal(signal.SIGINT, stop_query)
signal.signal(signal.SIGTERM, stop_query)

if __name__ == "__main__":
    # Initialize Spark session
    spark = (SparkSession.builder
        .appName("KafkaMongoDBStreaming")
        .master("spark://spark-master:7077")
        .config("spark.mongodb.input.uri", MONGO_URI)
        .config("spark.mongodb.output.uri", MONGO_URI)
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    # Set up streaming
    spark.sparkContext.setLogLevel("ERROR")
    
    stockDataframe = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()
    
    stockDataframe = stockDataframe.select(
        col("value").cast("string").alias("data")
    )
    
    inputStream = stockDataframe.selectExpr("CAST(data as STRING)")
    
    gameDataframe = inputStream.select(
        from_json(col("data"), schema).alias("parsed_data")
    )
    
    expandedDf = gameDataframe.select("parsed_data.*")
    
    # Start streaming
    query = expandedDf \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoints") \
        .start()

    try:
        query.awaitTermination()
    except Exception as e:
        print(f"Error in streaming query: {str(e)}")
        traceback.print_exc()
    finally:
        print("Shutting down Spark application...")
        if 'query' in globals():
            query.stop()
        if 'spark' in globals():
            spark.stop()