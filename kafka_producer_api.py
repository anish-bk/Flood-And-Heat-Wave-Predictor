"""
Kafka Weather Data Producer with REST API
Receives weather data via API and publishes to Kafka topic.

Usage:
    python kafka_producer_api.py

API Endpoints:
    POST /weather - Submit single weather observation
    POST /weather/batch - Submit batch of observations
    GET /health - Health check

Start Kafka with Docker:
    docker-compose up -d
"""

import os
import json
import time
from datetime import datetime
from typing import Optional, List
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import warnings
warnings.filterwarnings('ignore')

# Try importing Kafka
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️  kafka-python not installed. Run: pip install kafka-python")


# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic': 'weather-data',
    'api_host': '0.0.0.0',
    'api_port': 8000
}


# ============================================================================
# DATA MODELS
# ============================================================================

class WeatherObservation(BaseModel):
    """Single weather observation from a station."""
    district: str
    date: str  # YYYY-MM-DD format
    latitude: Optional[float] = 27.0
    longitude: Optional[float] = 85.0
    max_temp: float  # MaxTemp_2m
    min_temp: Optional[float] = None
    temp_range: Optional[float] = None
    precipitation: float  # Precip
    humidity: float  # RH_2m
    wind_speed_10m: Optional[float] = 0.0
    wind_speed_50m: Optional[float] = 0.0
    solar_radiation: Optional[float] = 0.0
    
    class Config:
        json_schema_extra = {
            "example": {
                "district": "Chitawan",
                "date": "2024-06-15",
                "latitude": 27.7,
                "longitude": 84.4,
                "max_temp": 42.5,
                "precipitation": 5.2,
                "humidity": 75.0,
                "wind_speed_10m": 3.5,
                "wind_speed_50m": 8.2
            }
        }


class WeatherBatch(BaseModel):
    """Batch of weather observations."""
    observations: List[WeatherObservation]


class ProducerStats(BaseModel):
    """Producer statistics."""
    messages_sent: int
    errors: int
    last_message_time: Optional[str]
    kafka_connected: bool


# ============================================================================
# KAFKA PRODUCER
# ============================================================================

class WeatherKafkaProducer:
    """Kafka producer for weather data."""
    
    def __init__(self, bootstrap_servers: List[str], topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.connected = False
        self.stats = {
            'messages_sent': 0,
            'errors': 0,
            'last_message_time': None
        }
    
    def connect(self) -> bool:
        """Connect to Kafka broker."""
        if not KAFKA_AVAILABLE:
            print("❌ kafka-python not available")
            return False
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3
            )
            self.connected = True
            print(f"✅ Connected to Kafka at {self.bootstrap_servers}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            self.connected = False
            return False
    
    def send_observation(self, observation: WeatherObservation) -> bool:
        """Send a single observation to Kafka."""
        if not self.connected:
            self.stats['errors'] += 1
            return False
        
        try:
            # Convert to dict with standardized field names
            message = {
                'District': observation.district,
                'Date': observation.date,
                'Latitude': observation.latitude,
                'Longitude': observation.longitude,
                'MaxTemp_2m': observation.max_temp,
                'MinTemp_2m': observation.min_temp or (observation.max_temp - 10),
                'TempRange_2m': observation.temp_range or 10.0,
                'Precip': observation.precipitation,
                'RH_2m': observation.humidity,
                'WindSpeed_10m': observation.wind_speed_10m,
                'WindSpeed_50m': observation.wind_speed_50m,
                'SolarRad': observation.solar_radiation,
                'timestamp': datetime.now().isoformat(),
                'source': 'api'
            }
            
            # Use district as key for partitioning
            future = self.producer.send(
                self.topic,
                key=observation.district,
                value=message
            )
            
            # Wait for send to complete
            future.get(timeout=10)
            
            self.stats['messages_sent'] += 1
            self.stats['last_message_time'] = datetime.now().isoformat()
            
            return True
            
        except KafkaError as e:
            print(f"❌ Kafka error: {e}")
            self.stats['errors'] += 1
            return False
        except Exception as e:
            print(f"❌ Error sending message: {e}")
            self.stats['errors'] += 1
            return False
    
    def send_batch(self, observations: List[WeatherObservation]) -> int:
        """Send batch of observations."""
        sent = 0
        for obs in observations:
            if self.send_observation(obs):
                sent += 1
        self.producer.flush()  # Ensure all messages are sent
        return sent
    
    def get_stats(self) -> ProducerStats:
        """Get producer statistics."""
        return ProducerStats(
            messages_sent=self.stats['messages_sent'],
            errors=self.stats['errors'],
            last_message_time=self.stats['last_message_time'],
            kafka_connected=self.connected
        )
    
    def close(self):
        """Close producer connection."""
        if self.producer:
            self.producer.close()
            self.connected = False


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

app = FastAPI(
    title="Weather Data Kafka Producer API",
    description="REST API to publish weather observations to Kafka for real-time prediction",
    version="1.0.0"
)

# Global producer instance
producer = WeatherKafkaProducer(
    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
    topic=KAFKA_CONFIG['topic']
)


@app.on_event("startup")
async def startup_event():
    """Initialize Kafka producer on startup."""
    print("\n🚀 Starting Weather Kafka Producer API...")
    if KAFKA_AVAILABLE:
        producer.connect()
    else:
        print("⚠️  Running in simulation mode (no Kafka)")


@app.on_event("shutdown")
async def shutdown_event():
    """Close Kafka producer on shutdown."""
    producer.close()
    print("👋 Producer shut down")


@app.get("/", tags=["Info"])
async def root():
    """API root - returns basic info."""
    return {
        "name": "Weather Kafka Producer API",
        "version": "1.0.0",
        "kafka_topic": KAFKA_CONFIG['topic'],
        "kafka_connected": producer.connected,
        "endpoints": {
            "POST /weather": "Submit single observation",
            "POST /weather/batch": "Submit batch of observations",
            "GET /stats": "Get producer statistics",
            "GET /health": "Health check"
        }
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "kafka_connected": producer.connected,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/stats", response_model=ProducerStats, tags=["Statistics"])
async def get_stats():
    """Get producer statistics."""
    return producer.get_stats()


@app.post("/weather", tags=["Weather Data"])
async def submit_weather(observation: WeatherObservation):
    """
    Submit a single weather observation to Kafka.
    
    The observation will be published to the 'weather-data' topic
    and consumed by the prediction service.
    """
    if not producer.connected and KAFKA_AVAILABLE:
        # Try to reconnect
        producer.connect()
    
    if producer.connected:
        success = producer.send_observation(observation)
        if success:
            return {
                "status": "success",
                "message": f"Weather data for {observation.district} published to Kafka",
                "topic": KAFKA_CONFIG['topic'],
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to publish to Kafka")
    else:
        # Simulation mode - just acknowledge
        return {
            "status": "simulated",
            "message": f"Weather data for {observation.district} received (Kafka not connected)",
            "data": observation.dict(),
            "timestamp": datetime.now().isoformat()
        }


@app.post("/weather/batch", tags=["Weather Data"])
async def submit_weather_batch(batch: WeatherBatch):
    """
    Submit a batch of weather observations to Kafka.
    
    More efficient for bulk data ingestion.
    """
    if not producer.connected and KAFKA_AVAILABLE:
        producer.connect()
    
    if producer.connected:
        sent = producer.send_batch(batch.observations)
        return {
            "status": "success",
            "message": f"Published {sent}/{len(batch.observations)} observations to Kafka",
            "topic": KAFKA_CONFIG['topic'],
            "timestamp": datetime.now().isoformat()
        }
    else:
        return {
            "status": "simulated",
            "message": f"Received {len(batch.observations)} observations (Kafka not connected)",
            "timestamp": datetime.now().isoformat()
        }


@app.post("/weather/simulate", tags=["Testing"])
async def simulate_stream(district: str = "Chitawan", count: int = 10, interval_ms: int = 500):
    """
    Simulate a stream of weather observations for testing.
    
    Generates random weather data and publishes to Kafka.
    """
    import random
    
    results = []
    for i in range(count):
        obs = WeatherObservation(
            district=district,
            date=datetime.now().strftime("%Y-%m-%d"),
            latitude=27.0 + random.uniform(-1, 1),
            longitude=85.0 + random.uniform(-1, 1),
            max_temp=30 + random.uniform(-5, 15),
            precipitation=random.uniform(0, 50),
            humidity=60 + random.uniform(-20, 30),
            wind_speed_10m=random.uniform(0, 10),
            wind_speed_50m=random.uniform(5, 20)
        )
        
        if producer.connected:
            producer.send_observation(obs)
        
        results.append({
            "index": i + 1,
            "district": obs.district,
            "max_temp": obs.max_temp,
            "precipitation": obs.precipitation
        })
        
        time.sleep(interval_ms / 1000)
    
    return {
        "status": "success",
        "simulated_count": count,
        "district": district,
        "samples": results[:5]  # Return first 5 as sample
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Run the API server."""
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   🌡️  WEATHER DATA KAFKA PRODUCER API                                        ║
║                                                                              ║
║   Publishes weather observations to Kafka for real-time prediction           ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📡 API Endpoints:
   POST /weather         - Submit single observation
   POST /weather/batch   - Submit batch of observations
   POST /weather/simulate - Simulate stream for testing
   GET  /stats           - Get producer statistics
   GET  /health          - Health check

🔗 Kafka Topic: {topic}

💡 Start Kafka first:
   docker-compose up -d

📖 API Docs: http://localhost:{port}/docs
""".format(topic=KAFKA_CONFIG['topic'], port=KAFKA_CONFIG['api_port']))
    
    uvicorn.run(
        app,
        host=KAFKA_CONFIG['api_host'],
        port=KAFKA_CONFIG['api_port'],
        log_level="info"
    )


if __name__ == "__main__":
    main()
