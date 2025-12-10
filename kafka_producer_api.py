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
import requests
from datetime import datetime
from typing import Optional, List
import uvicorn
from fastapi import FastAPI, HTTPException, Query
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
    'api_host': 'localhost',
    # 'api_host': '0.0.0.0',
    'api_port': 8000
}

# OpenWeatherMap Configuration
OPENWEATHER_CONFIG = {
    'api_key': os.getenv('OPENWEATHERMAP_API_KEY', ''),  # Set your API key as environment variable
    'base_url': 'https://api.openweathermap.org/data/2.5/weather'
}

# Nepal districts with approximate coordinates for OpenWeatherMap
NEPAL_CITIES = {
    'Kathmandu': {'lat': 27.7172, 'lon': 85.3240},
    'Pokhara': {'lat': 28.2096, 'lon': 83.9856},
    'Chitawan': {'lat': 27.5291, 'lon': 84.3542},
    'Biratnagar': {'lat': 26.4525, 'lon': 87.2718},
    'Lalitpur': {'lat': 27.6588, 'lon': 85.3247},
    'Bharatpur': {'lat': 27.6833, 'lon': 84.4333},
    'Birgunj': {'lat': 27.0104, 'lon': 84.8821},
    'Butwal': {'lat': 27.7006, 'lon': 83.4483},
    'Dharan': {'lat': 26.8065, 'lon': 87.2846},
    'Bhaktapur': {'lat': 27.6710, 'lon': 85.4298},
    'Janakpur': {'lat': 26.7288, 'lon': 85.9263},
    'Hetauda': {'lat': 27.4167, 'lon': 85.0333},
    'Nepalgunj': {'lat': 28.0500, 'lon': 81.6167},
    'Siraha': {'lat': 26.6544, 'lon': 86.2009},
    'Dhangadhi': {'lat': 28.6833, 'lon': 80.6000},
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
# OPENWEATHERMAP API INTEGRATION
# ============================================================================

def fetch_openweathermap_data(city: str) -> dict:
    """
    Fetch weather data from OpenWeatherMap API.
    
    Args:
        city: City name to fetch weather for
        
    Returns:
        Dictionary with weather data or None if failed
    """
    api_key = OPENWEATHER_CONFIG['api_key']
    
    if not api_key:
        raise HTTPException(
            status_code=500, 
            detail="OpenWeatherMap API key not configured. Set OPENWEATHERMAP_API_KEY environment variable."
        )
    
    url = f"{OPENWEATHER_CONFIG['base_url']}?q={city}&appid={api_key}"
    
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if data.get('cod') == 404 or data.get('cod') == '404':
            raise HTTPException(status_code=404, detail=f"City not found: {city}")
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=data.get('message', 'API error'))
        
        return data
        
    except requests.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Failed to connect to OpenWeatherMap: {str(e)}")


def parse_openweathermap_response(data: dict, city: str) -> dict:
    """
    Parse OpenWeatherMap API response into our weather format.
    
    Args:
        data: Raw API response
        city: Original city name requested
        
    Returns:
        Parsed weather data dictionary
    """
    # Extract temperature (convert from Kelvin to Celsius)
    main = data.get('main', {})
    temp_k = main.get('temp', 273.15)
    temp_c = temp_k - 273.15
    temp_max_c = main.get('temp_max', temp_k) - 273.15
    temp_min_c = main.get('temp_min', temp_k) - 273.15
    feels_like_c = main.get('feels_like', temp_k) - 273.15
    
    # Extract other data
    humidity = main.get('humidity', 50)
    pressure = main.get('pressure', 1013)
    
    wind = data.get('wind', {})
    wind_speed = wind.get('speed', 0)  # m/s
    wind_deg = wind.get('deg', 0)
    
    clouds = data.get('clouds', {})
    cloudiness = clouds.get('all', 0)
    
    visibility = data.get('visibility', 10000)  # meters
    
    # Rain data (if available)
    rain = data.get('rain', {})
    precipitation_1h = rain.get('1h', 0)  # mm in last hour
    precipitation_3h = rain.get('3h', 0)  # mm in last 3 hours
    # Estimate daily precipitation
    precipitation = precipitation_1h * 24 if precipitation_1h else precipitation_3h * 8 if precipitation_3h else 0
    
    # Weather description
    weather_list = data.get('weather', [{}])
    weather_desc = weather_list[0].get('description', 'N/A') if weather_list else 'N/A'
    weather_main = weather_list[0].get('main', 'N/A') if weather_list else 'N/A'
    
    # Location data
    coord = data.get('coord', {})
    latitude = coord.get('lat', 27.0)
    longitude = coord.get('lon', 85.0)
    
    # Sunrise/Sunset
    sys = data.get('sys', {})
    sunrise_ts = sys.get('sunrise')
    sunset_ts = sys.get('sunset')
    sunrise = datetime.fromtimestamp(sunrise_ts).strftime('%H:%M') if sunrise_ts else 'N/A'
    sunset = datetime.fromtimestamp(sunset_ts).strftime('%H:%M') if sunset_ts else 'N/A'
    
    return {
        'city_name': data.get('name', city),
        'district': city,
        'date': datetime.now().strftime('%Y-%m-%d'),
        'datetime': datetime.now().strftime('%d %b %Y | %I:%M:%S %p'),
        'latitude': latitude,
        'longitude': longitude,
        'temperature': round(temp_c, 1),
        'temp_max': round(temp_max_c, 1),
        'temp_min': round(temp_min_c, 1),
        'feels_like': round(feels_like_c, 1),
        'temp_range': round(temp_max_c - temp_min_c, 1),
        'humidity': humidity,
        'pressure': pressure,
        'precipitation': round(precipitation, 1),
        'wind_speed': round(wind_speed, 1),
        'wind_direction': wind_deg,
        'cloudiness': cloudiness,
        'visibility': visibility,
        'visibility_km': round(visibility / 1000, 1),
        'weather_main': weather_main,
        'weather_description': weather_desc,
        'sunrise': sunrise,
        'sunset': sunset,
        'source': 'openweathermap'
    }


@app.get("/weather/city/{city}", tags=["OpenWeatherMap"])
async def get_city_weather(city: str):
    """
    Fetch current weather for a city from OpenWeatherMap API.
    
    Returns the weather data without publishing to Kafka.
    Use POST /weather/city/{city} to also publish to Kafka.
    """
    data = fetch_openweathermap_data(city)
    parsed = parse_openweathermap_response(data, city)
    
    return {
        "status": "success",
        "source": "openweathermap",
        "weather": parsed
    }


@app.post("/weather/city/{city}", tags=["OpenWeatherMap"])
async def fetch_and_publish_city_weather(city: str, publish_to_kafka: bool = True):
    """
    Fetch weather for a city from OpenWeatherMap and publish to Kafka.
    
    This endpoint:
    1. Fetches real-time weather data from OpenWeatherMap
    2. Parses and transforms the data
    3. Publishes to Kafka topic for prediction pipeline
    
    Args:
        city: City name (e.g., 'Kathmandu', 'Siraha', 'Pokhara')
        publish_to_kafka: Whether to publish to Kafka (default: True)
    """
    # Fetch from OpenWeatherMap
    data = fetch_openweathermap_data(city)
    parsed = parse_openweathermap_response(data, city)
    
    # Display weather info
    print("\n" + "="*60)
    print(f"Weather Stats for - {parsed['city_name'].upper()}  || {parsed['datetime']}")
    print("="*60)
    print(f"Current temperature: {parsed['temperature']}°C")
    print(f"Feels like: {parsed['feels_like']}°C")
    print(f"Temperature range: {parsed['temp_min']}°C - {parsed['temp_max']}°C")
    print(f"Weather: {parsed['weather_description'].title()}")
    print(f"Humidity: {parsed['humidity']}%")
    print(f"Pressure: {parsed['pressure']} hPa")
    print(f"Wind: {parsed['wind_speed']} m/s at {parsed['wind_direction']}°")
    print(f"Visibility: {parsed['visibility_km']} km")
    print(f"Cloudiness: {parsed['cloudiness']}%")
    print(f"Sunrise: {parsed['sunrise']} | Sunset: {parsed['sunset']}")
    print("="*60 + "\n")
    
    result = {
        "status": "success",
        "source": "openweathermap",
        "weather": parsed,
        "kafka_published": False
    }
    
    # Publish to Kafka if requested
    if publish_to_kafka:
        obs = WeatherObservation(
            district=parsed['district'],
            date=parsed['date'],
            latitude=parsed['latitude'],
            longitude=parsed['longitude'],
            max_temp=parsed['temp_max'],
            min_temp=parsed['temp_min'],
            temp_range=parsed['temp_range'],
            precipitation=parsed['precipitation'],
            humidity=parsed['humidity'],
            wind_speed_10m=parsed['wind_speed'],
            wind_speed_50m=parsed['wind_speed'] * 1.5  # Estimate 50m wind
        )
        
        if producer.connected:
            success = producer.send_observation(obs)
            result['kafka_published'] = success
            result['kafka_topic'] = KAFKA_CONFIG['topic']
            if success:
                print(f"✅ Published to Kafka topic: {KAFKA_CONFIG['topic']}")
        else:
            result['kafka_message'] = "Kafka not connected (simulation mode)"
            print("⚠️  Kafka not connected - data not published")
    
    return result


@app.post("/weather/cities", tags=["OpenWeatherMap"])
async def fetch_multiple_cities(
    cities: List[str] = Query(default=["Kathmandu", "Pokhara", "Chitawan"]),
    interval_ms: int = 1000
):
    """
    Fetch weather for multiple cities and publish to Kafka.
    
    Args:
        cities: List of city names
        interval_ms: Delay between API calls (to avoid rate limiting)
    """
    results = []
    
    for city in cities:
        try:
            data = fetch_openweathermap_data(city)
            parsed = parse_openweathermap_response(data, city)
            
            # Create observation and publish
            obs = WeatherObservation(
                district=parsed['district'],
                date=parsed['date'],
                latitude=parsed['latitude'],
                longitude=parsed['longitude'],
                max_temp=parsed['temp_max'],
                min_temp=parsed['temp_min'],
                temp_range=parsed['temp_range'],
                precipitation=parsed['precipitation'],
                humidity=parsed['humidity'],
                wind_speed_10m=parsed['wind_speed'],
                wind_speed_50m=parsed['wind_speed'] * 1.5
            )
            
            kafka_success = False
            if producer.connected:
                kafka_success = producer.send_observation(obs)
            
            results.append({
                "city": city,
                "status": "success",
                "temperature": parsed['temperature'],
                "humidity": parsed['humidity'],
                "weather": parsed['weather_description'],
                "kafka_published": kafka_success
            })
            
            print(f"✅ {city}: {parsed['temperature']}°C, {parsed['humidity']}% humidity")
            
        except HTTPException as e:
            results.append({
                "city": city,
                "status": "error",
                "error": e.detail
            })
            print(f"❌ {city}: {e.detail}")
        
        # Rate limiting delay
        time.sleep(interval_ms / 1000)
    
    return {
        "status": "completed",
        "total_cities": len(cities),
        "successful": len([r for r in results if r['status'] == 'success']),
        "results": results
    }


@app.get("/weather/nepal", tags=["OpenWeatherMap"])
async def get_nepal_weather():
    """
    Fetch weather for major Nepal cities and publish to Kafka.
    
    Uses predefined list of Nepal cities/districts.
    """
    results = []
    
    for city in list(NEPAL_CITIES.keys())[:5]:  # Limit to 5 to avoid rate limiting
        try:
            data = fetch_openweathermap_data(city)
            parsed = parse_openweathermap_response(data, city)
            
            obs = WeatherObservation(
                district=city,
                date=parsed['date'],
                latitude=parsed['latitude'],
                longitude=parsed['longitude'],
                max_temp=parsed['temp_max'],
                min_temp=parsed['temp_min'],
                precipitation=parsed['precipitation'],
                humidity=parsed['humidity'],
                wind_speed_10m=parsed['wind_speed']
            )
            
            kafka_success = False
            if producer.connected:
                kafka_success = producer.send_observation(obs)
            
            results.append({
                "city": city,
                "temperature": parsed['temperature'],
                "humidity": parsed['humidity'],
                "weather": parsed['weather_description'],
                "kafka_published": kafka_success
            })
            
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            results.append({"city": city, "error": str(e)})
    
    return {
        "status": "success",
        "source": "openweathermap",
        "cities_fetched": len(results),
        "results": results
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Run the API server."""
    api_key_status = "✅ Configured" if OPENWEATHER_CONFIG['api_key'] else "❌ Not set (set OPENWEATHERMAP_API_KEY)"
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   🌡️  WEATHER DATA KAFKA PRODUCER API                                        ║
║                                                                              ║
║   Fetches real weather from OpenWeatherMap & publishes to Kafka              ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📡 API Endpoints:
   POST /weather              - Submit manual observation
   POST /weather/batch        - Submit batch of observations
   POST /weather/simulate     - Simulate stream for testing
   
🌤️ OpenWeatherMap Endpoints:
   GET  /weather/city/{{city}}  - Get weather for a city
   POST /weather/city/{{city}}  - Fetch & publish city weather to Kafka
   POST /weather/cities        - Fetch multiple cities
   GET  /weather/nepal         - Fetch major Nepal cities
   
   GET  /stats                - Get producer statistics
   GET  /health               - Health check

🔗 Kafka Topic: {topic}
🌤️ OpenWeatherMap API Key: {api_key_status}

💡 Setup:
   1. Set API key: $env:OPENWEATHERMAP_API_KEY="your_key"
   2. Start Kafka: docker-compose up -d
   3. Test: POST /weather/city/Kathmandu

📖 API Docs: http://localhost:{port}/docs
""".format(
        topic=KAFKA_CONFIG['topic'], 
        port=KAFKA_CONFIG['api_port'],
        api_key_status=api_key_status
    ))
    
    uvicorn.run(
        app,
        host=KAFKA_CONFIG['api_host'],
        port=KAFKA_CONFIG['api_port'],
        log_level="info"
    )


if __name__ == "__main__":
    main()
