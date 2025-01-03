# Import necessary libraries for data processing and streaming
import sys
import json
import findspark
import pyarrow
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
    """Training phase class - handles model training and persistence with continuous updates"""
    def __init__(self, spark):
        self.spark = spark
        # Initialize the ALS (Alternating Least Squares) model with fixed parameters
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
        # Track accumulated training data across batches
        self.accumulated_training_data = None

    # def load_or_initialize_model(self, mongo_client):
    #     """Load existing model or initialize a new one if none exists."""
    #     try:
    #         db = mongo_client[MONGO_DATABASE]
    #         model_collection = db[MODEL_COLLECTION]
            
    #         # Try to find the latest model
    #         latest_model = model_collection.find_one(sort=[('timestamp', -1)])
            
    #         if latest_model and os.path.exists(latest_model['model_path']):
    #             print("Loading existing model...")
    #             self.model = ALS.load(latest_model['model_path'])
                
    #             # Convert stored mappings back to Spark DataFrames
    #             mapping_pdf = pd.DataFrame(latest_model['deck_pattern_mapping'])
    #             self.deck_pattern_mapping = self.spark.createDataFrame(mapping_pdf)
                
    #             matrix_pdf = pd.DataFrame(latest_model['card_matrix'])
    #             self.card_matrix = self.spark.createDataFrame(matrix_pdf)
                
    #             # Load accumulated training data
    #             if 'accumulated_training_data' in latest_model:
    #                 training_pdf = pd.DataFrame(latest_model['accumulated_training_data'])
    #                 self.accumulated_training_data = self.spark.createDataFrame(training_pdf)
                
    #             print("Existing model and data loaded successfully")
    #             return True
            
    #         print("No existing model found, will initialize new one with first batch")
    #         return False
            
    #     except Exception as e:
    #         print(f"Error loading model: {str(e)}")
    #         traceback.print_exc()
    #         return False

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
        """Generate training sequences from a deck with performance weighting."""
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

            # Create or update mapping of deck patterns to numeric IDs
            # Create or update mapping of deck patterns to numeric IDs
            window = Window.orderBy("deck_pattern")
            if self.deck_pattern_mapping is None:
                deck_patterns = all_sequences.select("deck_pattern").distinct()
                self.deck_pattern_mapping = deck_patterns.withColumn(
                    "deck_pattern_id",
                    F.row_number().over(window).cast("integer")
                )
            else:
                # Add new patterns to existing mapping
                new_patterns = all_sequences.select("deck_pattern").distinct()
                existing_patterns = self.deck_pattern_mapping.select("deck_pattern")
                new_unique_patterns = new_patterns.subtract(existing_patterns)
                
                if new_unique_patterns.count() > 0:
                    # Get current maximum ID
                    max_id = self.deck_pattern_mapping.agg(F.max("deck_pattern_id")).collect()[0][0]
                    
                    # Add new patterns with controlled IDs
                    new_mapping = new_unique_patterns.withColumn(
                        "deck_pattern_id",
                        (F.row_number().over(window) + max_id).cast("integer")
                    )
                    self.deck_pattern_mapping = self.deck_pattern_mapping.union(new_mapping)

            # Join with mapping to get numeric IDs
            all_sequences = all_sequences.join(
                self.deck_pattern_mapping,
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
            
            # Update card matrix
            self.card_matrix = training_data
            return training_data

        except Exception as e:
            print(f"Error in prepare_training_data: {str(e)}")
            traceback.print_exc()
            raise

    def update_training_data(self, new_training_data):
        """Update accumulated training data with new batch data."""
        if self.accumulated_training_data is None:
            self.accumulated_training_data = new_training_data
        else:
            # Combine existing and new data
            combined_data = self.accumulated_training_data.unionAll(new_training_data)
            
            # Recalculate aggregates
            window_spec = Window.partitionBy("deck_pattern_id")
            self.accumulated_training_data = combined_data.groupBy(
                "deck_pattern_id", 
                "next_card"
            ).agg(
                F.sum("total_appearances").alias("total_appearances"),
                F.avg("weighted_performance").alias("weighted_performance"),
                F.avg("performance_consistency").alias("performance_consistency"),
                F.avg("avg_sequence_length").alias("avg_sequence_length")
            ).withColumn(
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

    def train(self, training_data):
        """Train the recommendation model on accumulated data."""
        self.model = self.als.fit(training_data)

    # def save_model_to_mongodb(self, mongo_client):
    #     """Save the updated model and accumulated data."""
    #     try:
    #         db = mongo_client[MONGO_DATABASE]
    #         model_collection = db[MODEL_COLLECTION]
            
    #         # Use consistent model name for updates
    #         model_name = "continuous_als_model"
    #         model_dir = os.path.join(MODEL_BASE_DIR, model_name)
    #         os.makedirs(model_dir, exist_ok=True)
            
    #         # Save the model
    #         self.model.write().overwrite().save(model_dir)
            
    #         # Store metadata and accumulated data
    #         model_data = {
    #             'timestamp': datetime.now(),
    #             'model_name': model_name,
    #             'model_path': model_dir,
    #             'deck_pattern_mapping': self.deck_pattern_mapping.toPandas().to_dict('records'),
    #             'card_matrix': self.card_matrix.toPandas().to_dict('records'),
    #             'accumulated_training_data': self.accumulated_training_data.toPandas().to_dict('records')
    #         }
            
    #         # Update in MongoDB (using upsert to ensure single document)
    #         model_collection.update_one(
    #             {'model_name': model_name},
    #             {'$set': model_data},
    #             upsert=True
    #         )
    #         print(f"Model and accumulated data successfully saved to {model_dir}")
            
    #     except Exception as e:
    #         print(f"Error saving model: {str(e)}")
    #         traceback.print_exc()
    #         raise
    def save_model_to_mongodb(self, mongo_client):
        try:
            db = mongo_client[MONGO_DATABASE]
            model_collection = db[MODEL_COLLECTION]
            
            # Use consistent model name and paths
            model_name = "continuous_als_model"
            model_dir = os.path.join(MODEL_BASE_DIR, model_name)
            os.makedirs(model_dir, exist_ok=True)
            
            # Save the model
            model_path = os.path.join(model_dir, "als_model")
            self.model.write().overwrite().save(model_path)
            
            # Save DataFrames as single files with specific names
            mapping_path = os.path.join(model_dir, "deck_pattern_mapping.parquet")
            matrix_path = os.path.join(model_dir, "card_matrix.parquet")
            training_path = os.path.join(model_dir, "accumulated_training_data.parquet")
            
            print("Before saving:")
            print(f"Deck pattern mapping count: {self.deck_pattern_mapping.count()}")
            print(f"Card matrix count: {self.card_matrix.count()}")
            print(f"Accumulated training data count: {self.accumulated_training_data.count()}")
            
            # Convert to pandas and save directly
            (self.deck_pattern_mapping
            .toPandas()
            .to_parquet(mapping_path, index=False))
            
            (self.card_matrix
            .toPandas()
            .to_parquet(matrix_path, index=False))
            
            (self.accumulated_training_data
            .toPandas()
            .to_parquet(training_path, index=False))
            
            print("\nAfter saving:")
            for path, name in [(mapping_path, "mapping"), (matrix_path, "matrix"), (training_path, "training")]:
                if os.path.exists(path):
                    size = os.path.getsize(path)
                    print(f"{name} file size: {size} bytes")
                    # Try to read back and verify
                    import pandas as pd
                    df = pd.read_parquet(path)
                    print(f"{name} columns: {df.columns.tolist()}")
                    print(f"{name} rows: {len(df)}")
                else:
                    print(f"{name} file not found!")
            
            # Store metadata in MongoDB
            model_metadata = {
                'timestamp': datetime.now(),
                'model_name': model_name,
                'model_path': model_path,
                'mapping_path': mapping_path,
                'matrix_path': matrix_path,
                'training_path': training_path,
                'total_patterns': self.deck_pattern_mapping.count(),
                'total_training_samples': self.accumulated_training_data.count()
            }
            
            # Update in MongoDB
            model_collection.update_one(
                {'model_name': model_name},
                {'$set': model_metadata},
                upsert=True
            )
            print(f"\nModel and data successfully saved to {model_dir}")
            print(f"Total patterns: {model_metadata['total_patterns']}")
            print(f"Total training samples: {model_metadata['total_training_samples']}")
            
        except Exception as e:
            print(f"Error saving model: {str(e)}")
            traceback.print_exc()
            raise

    def load_or_initialize_model(self, mongo_client):
        """Load existing model or initialize a new one if none exists."""
        try:
            db = mongo_client[MONGO_DATABASE]
            model_collection = db[MODEL_COLLECTION]
            
            # Try to find the model metadata
            model_metadata = model_collection.find_one({'model_name': 'continuous_als_model'})
            
            if model_metadata and os.path.exists(model_metadata['model_path']):
                print("Loading existing model...")
                
                # Load the ALS model
                self.model = ALS.load(model_metadata['model_path'])
                
                # Load DataFrames from parquet files
                self.deck_pattern_mapping = self.spark.read.parquet(model_metadata['mapping_path'])
                self.card_matrix = self.spark.read.parquet(model_metadata['matrix_path'])
                self.accumulated_training_data = self.spark.read.parquet(model_metadata['training_path'])
                
                print("Existing model and data loaded successfully")
                print(f"Loaded {model_metadata['total_patterns']} patterns")
                print(f"Loaded {model_metadata['total_training_samples']} training samples")
                return True
            
            print("No existing model found, will initialize new one with first batch")
            return False
            
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            traceback.print_exc()
            return False


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
            # Get model metadata from MongoDB - specifically looking for our continuous model
            model_data = self.db[MODEL_COLLECTION].find_one(
                {'model_name': 'continuous_als_model'}
            )
            
            if not model_data:
                raise ValueError("No trained model found in database")

            model_dir = model_data['model_path']
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
        """
        Get card recommendations for a partial deck.
        
        Args:
            partial_deck (list): List of card IDs representing the current deck
            num_recommendations (int): Number of card recommendations to return
            
        Returns:
            list: List of dictionaries containing recommended cards and their scores
        """
        if not self.model or not self.deck_pattern_mapping:
            raise ValueError("Model not loaded")
            
        try:
            # Convert deck to pattern string for lookup
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
            
            # Generate recommendations using the trained model
            recommendations = self.model.recommendForUserSubset(
                pattern_with_id,
                num_recommendations
            )
            
            # Format recommendations with scores
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
    """Process streaming batch data and update existing model."""
    try:
        print(f"Processing batch {batch_id}")
        print(f"Batch size: {batch_df.count()}")
        
        # Set up MongoDB connection
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        game_collection = db[MONGO_COLLECTION]

        # Initialize or load recommender
        global card_recommender
        if 'card_recommender' not in globals():
            card_recommender = CardRecommender(spark)
            card_recommender.load_or_initialize_model(client)
            print("CardRecommender initialized/loaded")

        # Process batch and update model
        if batch_df.count() > 0:
            print(f"Processing {batch_df.count()} records")
            try:
                # Debug: Show sample of input data
                print("Sample of input data:")
                batch_df.show(2)
                
                # Prepare new training data
                new_training_data = card_recommender.prepare_training_data(batch_df)
                print(f"Generated {new_training_data.count()} training examples")
                print("Sample of training data:")
                new_training_data.show(2)
                
                if new_training_data.count() > 0:
                    # Update accumulated data
                    card_recommender.update_training_data(new_training_data)
                    print(f"Accumulated data size: {card_recommender.accumulated_training_data.count()}")
                    
                    # Train and save
                    card_recommender.train(card_recommender.accumulated_training_data)
                    card_recommender.save_model_to_mongodb(client)
                    print(f"Updated and saved card recommendation model")
                else:
                    print("No new training data generated")
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
    """Handle graceful shutdown of Spark streaming query."""
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
    # Initialize Spark session with optimized configuration
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

    # Set up streaming with error logging
    spark.sparkContext.setLogLevel("ERROR")
    
    # Read from Kafka stream
    stockDataframe = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON data from Kafka
    stockDataframe = stockDataframe.select(
        col("value").cast("string").alias("data")
    )
    
    inputStream = stockDataframe.selectExpr("CAST(data as STRING)")
    
    # Parse game data using defined schema
    gameDataframe = inputStream.select(
        from_json(col("data"), schema).alias("parsed_data")
    )
    
    # Extract nested fields
    expandedDf = gameDataframe.select("parsed_data.*")
    
    # Start streaming process
    query = expandedDf \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoints") \
        .start()

    try:
        # Wait for streaming to complete
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