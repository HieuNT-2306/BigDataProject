import os
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from typing import List, Optional, Dict
from pydantic import BaseModel, Field
from datetime import datetime
import logging
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pyarrow.parquet as pq

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Settings:
    MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT = os.getenv("MONGO_PORT", "27017")
    MONGO_USER = os.getenv("MONGO_USER", "root")
    MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "admin")
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"
    MONGO_DATABASE = "BigData"
    MODEL_COLLECTION = "card_recommendation_models"
    MODEL_BASE_DIR = os.getenv('MODEL_BASE_DIR', '/opt/spark/models')

class CardRecommendation(BaseModel):
    card_id: int
    score: float
    timestamp: datetime = Field(default_factory=datetime.now)

class RecommendationRequest(BaseModel):
    partial_deck: List[int] = Field(..., min_items=1, max_items=7)
    num_recommendations: Optional[int] = Field(default=3, ge=1, le=10)

class RecommendationResponse(BaseModel):
    recommendations: List[CardRecommendation]
    model_name: str

class RecommendationService:
    def __init__(self, mongo_uri: str):
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[Settings.MONGO_DATABASE]
        self.card_matrix = None
        self.deck_patterns = None
        self.model_name = "continuous_als_model"
        self.load_latest_data()

    def load_latest_data(self):
        """Load latest data from parquet files"""
        try:
            model_data = self.db[Settings.MODEL_COLLECTION].find_one(
                {'model_name': self.model_name},
                sort=[('timestamp', -1)]
            )
            
            if not model_data:
                raise ValueError("No model metadata found in database")

            # Load mapping from parquet
            mapping_path = model_data.get('mapping_path')
            logger.info(f"Loading mapping from: {mapping_path}")
            if os.path.exists(mapping_path):
                mapping_df = pd.read_parquet(mapping_path)
                logger.info(f"Mapping columns: {mapping_df.columns.tolist()}")
                self.deck_patterns = dict(zip(mapping_df['deck_pattern'], mapping_df['deck_pattern_id']))
                logger.info(f"Loaded {len(self.deck_patterns)} deck patterns")

            # Load matrix from parquet
            matrix_path = model_data.get('matrix_path')
            logger.info(f"Loading matrix from: {matrix_path}")
            if os.path.exists(matrix_path):
                self.card_matrix = pd.read_parquet(matrix_path)
                logger.info(f"Matrix shape: {self.card_matrix.shape}")
                logger.info(f"Matrix columns: {self.card_matrix.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise

    def find_best_matching_pattern(self, deck_pattern: str) -> Optional[str]:
        """Find the best matching pattern for input deck"""
        deck_cards = set(deck_pattern.split(','))
        
        best_match = None
        best_match_score = 0
        
        for pattern in self.deck_patterns.keys():
            pattern_cards = set(pattern.split(','))
            match_score = len(deck_cards.intersection(pattern_cards))
            
            if match_score > best_match_score:
                best_match_score = match_score
                best_match = pattern
                
        return best_match

    def find_matching_patterns(self, deck_pattern: str, top_n: int = 5) -> List[int]:
        deck_cards = set(deck_pattern.split(','))
        
        # Store pattern scores
        pattern_scores = []
        for pattern, pattern_id in self.deck_patterns.items():
            pattern_cards = set(pattern.split(','))
            # Calculate overlap score
            intersection = len(deck_cards.intersection(pattern_cards))
            union = len(deck_cards.union(pattern_cards))
            jaccard_score = intersection / union if union > 0 else 0
            
            pattern_scores.append({
                'pattern_id': pattern_id,
                'score': jaccard_score
            })
        
        # Sort by score and get top N
        sorted_patterns = sorted(pattern_scores, key=lambda x: x['score'], reverse=True)
        return [p['pattern_id'] for p in sorted_patterns[:top_n]]

    def get_recommendations(self, partial_deck: List[int], num_recommendations: int = 3) -> List[Dict]:
        """Get recommendations for a partial deck"""
        try:
            deck_pattern = ','.join(map(str, sorted(partial_deck)))
            logger.info(f"Finding recommendations for pattern: {deck_pattern}")
            
            # Get multiple matching patterns
            pattern_ids = self.find_matching_patterns(deck_pattern)
            if not pattern_ids:
                logger.warning("No matching patterns found")
                return []
                
            logger.info(f"Found {len(pattern_ids)} matching patterns")
            
            # Get recommendations from all matching patterns
            all_recs = []
            for pattern_id in pattern_ids:
                pattern_recs = self.card_matrix[self.card_matrix['deck_pattern_id'] == pattern_id]
                filtered_recs = pattern_recs[~pattern_recs['next_card'].isin(partial_deck)]
                all_recs.extend(filtered_recs.to_dict('records'))
            
            # If we still don't have enough recommendations, get popular cards
            if len(all_recs) < num_recommendations:
                logger.info("Not enough recommendations, adding popular cards")
                popular_cards = (self.card_matrix[~self.card_matrix['next_card'].isin(partial_deck)]
                            .groupby('next_card')
                            .agg({
                                'effectiveness_score': 'mean',
                                'total_appearances': 'sum'
                            })
                            .reset_index()
                            .sort_values('total_appearances', ascending=False)
                            )
                popular_recs = popular_cards.head(num_recommendations - len(all_recs))
                all_recs.extend(popular_recs.to_dict('records'))
            
            # Aggregate and sort recommendations
            from collections import defaultdict
            aggregated_recs = defaultdict(lambda: {'scores': [], 'appearances': 0})
            
            for rec in all_recs:
                card_id = rec['next_card']
                aggregated_recs[card_id]['scores'].append(rec['effectiveness_score'])
                aggregated_recs[card_id]['appearances'] += rec.get('total_appearances', 0)
            
            # Calculate final scores
            final_recs = []
            for card_id, data in aggregated_recs.items():
                avg_score = sum(data['scores']) / len(data['scores'])
                final_recs.append({
                    'card_id': int(card_id),
                    'score': float(avg_score),
                    'appearances': data['appearances']
                })
            
            # Sort by score and return top N
            sorted_recs = sorted(final_recs, key=lambda x: x['score'], reverse=True)[:num_recommendations]
            logger.info(f"Returning {len(sorted_recs)} recommendations")
            
            return [{
                'card_id': rec['card_id'],
                'score': rec['score'],
                'timestamp': datetime.now()
            } for rec in sorted_recs]
                
        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return []

# Initialize FastAPI app
app = FastAPI(title="Card Recommendation Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize service
recommendation_service = RecommendationService(Settings.MONGO_URI)

@app.post("/recommendations", response_model=RecommendationResponse)
async def get_recommendations(request: RecommendationRequest):
    """Get card recommendations for a partial deck"""
    try:
        recommendations = recommendation_service.get_recommendations(
            request.partial_deck,
            request.num_recommendations
        )
        
        return RecommendationResponse(
            recommendations=[CardRecommendation(**rec) for rec in recommendations],
            model_name=recommendation_service.model_name
        )
        
    except Exception as e:
        logger.error(f"Recommendation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "patterns_loaded": len(recommendation_service.deck_patterns or []),
        "matrix_shape": recommendation_service.card_matrix.shape if recommendation_service.card_matrix is not None else None,
        "timestamp": datetime.now()
    }

@app.get("/debug")
async def debug_info():
    """Debug endpoint to check data loading"""
    return {
        "deck_patterns_count": len(recommendation_service.deck_patterns or []),
        "deck_pattern_sample": list(recommendation_service.deck_patterns.keys())[:5] if recommendation_service.deck_patterns else None,
        "card_matrix_info": {
            "shape": recommendation_service.card_matrix.shape if recommendation_service.card_matrix is not None else None,
            "columns": recommendation_service.card_matrix.columns.tolist() if recommendation_service.card_matrix is not None else None,
            "sample": recommendation_service.card_matrix.head().to_dict('records') if recommendation_service.card_matrix is not None else None
        }
    }