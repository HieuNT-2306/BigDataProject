import os
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from typing import List, Optional, Dict, Set
import numpy as np
from pydantic import BaseModel, Field
from datetime import datetime
import logging
from fastapi.middleware.cors import CORSMiddleware

# Set up logging configuration to help with debugging and monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration settings using environment variables with sensible defaults
class Settings:
    MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT = os.getenv("MONGO_PORT", "27017")
    MONGO_USER = os.getenv("MONGO_USER", "root")
    MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "admin")
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"
    MONGO_DATABASE = "BigData"
    MODEL_COLLECTION = "card_recommendation_models"
    RECOMMENDATIONS_COLLECTION = "card_recommendations"

settings = Settings()

# Pydantic models for request and response validation
class CardRecommendation(BaseModel):
    card_id: int = Field(..., description="The ID of the recommended card")
    score: float = Field(..., ge=0, le=1, description="Recommendation confidence score")
    timestamp: datetime = Field(default_factory=datetime.now, description="When the recommendation was made")

class RecommendationRequest(BaseModel):
    partial_deck: List[int] = Field(..., 
                                  min_items=1, 
                                  max_items=7, 
                                  description="List of card IDs in the current deck")
    num_recommendations: Optional[int] = Field(default=3, 
                                             ge=1, 
                                             le=10, 
                                             description="Number of recommendations to return")

class RecommendationResponse(BaseModel):
    recommendations: List[CardRecommendation]
    model_timestamp: datetime

class OptimizedRecommendationService:
    """
    Service for providing card recommendations based on partial decks.
    Uses pre-computed patterns and effectiveness scores from MongoDB.
    """
    
    def __init__(self, mongo_uri: str):
        """Initialize the service with database connection and load model data"""
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[settings.MONGO_DATABASE]
        # Store pattern mappings and recommendations for quick access
        self.deck_patterns: Dict[str, int] = {}  # pattern -> id mapping
        self.recommendations: Dict[int, List[Dict]] = {}  # id -> recommendations
        self.model_timestamp: Optional[datetime] = None
        self.load_model()

    def load_model(self) -> None:
        """Load and process model data from MongoDB"""
        try:
            # Get the most recent model
            model_data = self.db[settings.MODEL_COLLECTION].find_one(
                sort=[('timestamp', -1)]
            )
            
            if not model_data:
                raise ValueError("No model found in database")

            # Process deck patterns for quick lookup
            for pattern in model_data['deck_pattern_mapping']:
                self.deck_patterns[pattern['deck_pattern']] = pattern['deck_pattern_id']

            # Organize recommendations by pattern ID
            for entry in model_data['card_matrix']:
                pattern_id = entry['deck_pattern_id']
                if pattern_id not in self.recommendations:
                    self.recommendations[pattern_id] = []
                
                # Store recommendation with its score
                self.recommendations[pattern_id].append({
                    'card_id': entry['next_card'],
                    'score': float(entry['effectiveness_score'])
                })

            self.model_timestamp = model_data['timestamp']
            logger.info(f"Successfully loaded {len(self.deck_patterns)} patterns "
                       f"and {len(self.recommendations)} recommendation sets")
            
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise

    def _find_best_matching_pattern(self, deck_pattern: str) -> Optional[str]:
        """
        Find the stored pattern that best matches the input deck pattern.
        Uses card overlap to determine the best match.
        """
        deck_cards = set(deck_pattern.split(','))
        
        best_match = None
        best_match_score = 0
        
        for pattern in self.deck_patterns.keys():
            pattern_cards = set(pattern.split(','))
            
            # Calculate card overlap
            match_score = len(deck_cards.intersection(pattern_cards))
            
            if match_score > best_match_score:
                best_match_score = match_score
                best_match = pattern
                
        return best_match

    def get_recommendations(
        self, 
        partial_deck: List[int], 
        num_recommendations: int = 3
    ) -> List[Dict]:
        """Generate card recommendations for a partial deck"""
        try:
            # Convert deck to pattern string
            deck_pattern = ','.join(map(str, sorted(partial_deck)))
            logger.info(f"Generating recommendations for deck: {deck_pattern}")
            
            # Find best matching pattern
            matching_pattern = self._find_best_matching_pattern(deck_pattern)
            
            if not matching_pattern:
                logger.warning(f"No matching pattern found for deck: {deck_pattern}")
                return []

            pattern_id = self.deck_patterns[matching_pattern]
            
            # Get and filter recommendations
            available_recs = self.recommendations.get(pattern_id, [])
            filtered_recs = [
                rec for rec in available_recs 
                if rec['card_id'] not in partial_deck
            ]
            
            # Sort by score and select top N
            sorted_recs = sorted(
                filtered_recs, 
                key=lambda x: x['score'], 
                reverse=True
            )[:num_recommendations]
            
            # Add timestamps
            return [{
                **rec, 
                'timestamp': datetime.now()
            } for rec in sorted_recs]
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {str(e)}")
            return []

# Initialize FastAPI app with CORS support
app = FastAPI(
    title="Card Recommendation Service",
    description="Optimized service for generating card recommendations",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize recommendation service
recommendation_service = OptimizedRecommendationService(settings.MONGO_URI)

@app.get("/health")
async def health_check():
    """Health check endpoint to verify service status"""
    return {
        "status": "healthy",
        "model_loaded": bool(recommendation_service.deck_patterns),
        "patterns_loaded": len(recommendation_service.deck_patterns),
        "timestamp": datetime.now()
    }

@app.get("/debug/model-data")
async def debug_model_data():
    """Debug endpoint to inspect model data structure"""
    try:
        model_data = recommendation_service.db[settings.MODEL_COLLECTION].find_one(
            sort=[('timestamp', -1)]
        )
        
        if not model_data:
            return {"status": "error", "message": "No model found"}

        # Create safe version of the data for display
        debug_data = {
            "available_keys": list(model_data.keys()),
            "model_structure": {
                "deck_pattern_mapping": {
                    "total_entries": len(model_data.get('deck_pattern_mapping', [])),
                    "sample_data": model_data.get('deck_pattern_mapping', [])[:2]
                },
                "card_matrix": {
                    "total_entries": len(model_data.get('card_matrix', [])),
                    "sample_data": [
                        {k: str(v) if isinstance(v, float) and (np.isnan(v) or np.isinf(v)) else v 
                         for k, v in entry.items()}
                        for entry in model_data.get('card_matrix', [])[:2]
                    ]
                }
            },
            "timestamp": model_data.get('timestamp', '').isoformat() if model_data.get('timestamp') else None,
            "model_name": model_data.get('model_name', 'Not found')
        }
        
        return {"status": "success", "data": debug_data}
        
    except Exception as e:
        logger.error(f"Error in debug endpoint: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "type": str(type(e).__name__)
        }

@app.post("/recommendations", response_model=RecommendationResponse)
async def get_recommendations(request: RecommendationRequest):
    """Generate card recommendations based on a partial deck"""
    try:
        recommendations = recommendation_service.get_recommendations(
            request.partial_deck,
            request.num_recommendations
        )
        
        # Store the recommendations for analysis
        recommendation_service.db[settings.RECOMMENDATIONS_COLLECTION].insert_one({
            'partial_deck': request.partial_deck,
            'recommendations': recommendations,
            'request_timestamp': datetime.now()
        })
        
        return RecommendationResponse(
            recommendations=[CardRecommendation(**rec) for rec in recommendations],
            model_timestamp=recommendation_service.model_timestamp
        )
        
    except Exception as e:
        logger.error(f"Error processing recommendation request: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)