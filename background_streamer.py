"""
Background Weather Data Streamer
================================
Continuously streams weather data from OpenWeatherMap for all districts
and stores it in Cassandra - simulating a distributed data collection system.

This runs independently of the Gradio dashboard, providing continuous
data ingestion from multiple sources (districts) simultaneously.

Usage:
    python background_streamer.py
    python background_streamer.py --interval 60
    python background_streamer.py --districts Bara,Dhanusa
"""

import os
import sys
import time
import signal
import argparse
import threading
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid
import json

# Cassandra
try:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    print("⚠️  cassandra-driver not installed. Run: pip install cassandra-driver")

# Kafka (optional - for publishing to Kafka as well)
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Configuration
CONFIG = {
    'openweathermap_api_key': os.environ.get('OPENWEATHERMAP_API_KEY', ''),
    'cassandra_hosts': ['127.0.0.1'],
    'cassandra_port': 9042,
    'cassandra_keyspace': 'weather_monitoring',
    'kafka_bootstrap_servers': 'localhost:9092',
    'kafka_topic': 'weather-data',
    'default_interval': 30,  # seconds between fetches per district
}

# Districts to monitor (Nepal Terai region)
DISTRICTS = {
    'Bara': {'lat': 27.0667, 'lon': 85.0667},
    'Dhanusa': {'lat': 26.8167, 'lon': 86.0000},
    'Sarlahi': {'lat': 26.8667, 'lon': 85.5833},
    'Parsa': {'lat': 27.1333, 'lon': 84.8833},
    'Siraha': {'lat': 26.6536, 'lon': 86.2019},
}


class CassandraStorage:
    """Handles Cassandra storage operations."""
    
    def __init__(self):
        self.cluster = None
        self.session = None
        self.connected = False
    
    def connect(self) -> bool:
        """Connect to Cassandra and setup schema."""
        if not CASSANDRA_AVAILABLE:
            return False
        
        try:
            self.cluster = Cluster(
                CONFIG['cassandra_hosts'],
                port=CONFIG['cassandra_port']
            )
            self.session = self.cluster.connect()
            
            # Create keyspace if not exists
            self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS weather_monitoring
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """)
            self.session.set_keyspace(CONFIG['cassandra_keyspace'])
            
            # Create tables
            self._setup_schema()
            self.connected = True
            return True
            
        except Exception as e:
            print(f"❌ Cassandra connection failed: {e}")
            return False
    
    def _setup_schema(self):
        """Create necessary tables."""
        # Weather observations table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS weather_observations (
                id UUID,
                district TEXT,
                date TEXT,
                fetch_time TIMESTAMP,
                max_temp DOUBLE,
                min_temp DOUBLE,
                temp_range DOUBLE,
                precipitation DOUBLE,
                humidity DOUBLE,
                wind_speed DOUBLE,
                pressure DOUBLE,
                cloudiness INT,
                visibility DOUBLE,
                weather_desc TEXT,
                sunrise TEXT,
                sunset TEXT,
                source TEXT,
                PRIMARY KEY ((district), fetch_time, id)
            ) WITH CLUSTERING ORDER BY (fetch_time DESC)
        """)
        
        # Predictions table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS weather_predictions (
                id UUID,
                district TEXT,
                prediction_time TIMESTAMP,
                max_temp DOUBLE,
                precipitation DOUBLE,
                humidity DOUBLE,
                heatwave_probability DOUBLE,
                flood_probability DOUBLE,
                heatwave_risk TEXT,
                flood_risk TEXT,
                PRIMARY KEY ((district), prediction_time, id)
            ) WITH CLUSTERING ORDER BY (prediction_time DESC)
        """)
        
        # Daily summary table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS daily_weather_summary (
                district TEXT,
                date TEXT,
                record_count INT,
                avg_temp DOUBLE,
                max_temp DOUBLE,
                min_temp DOUBLE,
                total_precip DOUBLE,
                avg_humidity DOUBLE,
                heatwave_alerts INT,
                flood_alerts INT,
                PRIMARY KEY ((district), date)
            ) WITH CLUSTERING ORDER BY (date DESC)
        """)
        
        # Streaming stats table (new - for tracking data ingestion)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS streaming_stats (
                stat_date TEXT,
                district TEXT,
                hour INT,
                record_count COUNTER,
                PRIMARY KEY ((stat_date), district, hour)
            )
        """)
    
    def store_observation(self, data: Dict) -> bool:
        """Store a weather observation."""
        if not self.connected:
            return False
        
        try:
            record_id = uuid.uuid4()
            fetch_time = datetime.fromisoformat(data.get('fetched_at', datetime.now().isoformat()))
            
            self.session.execute("""
                INSERT INTO weather_observations 
                (id, district, date, fetch_time, max_temp, min_temp, temp_range,
                 precipitation, humidity, wind_speed, pressure, cloudiness,
                 visibility, weather_desc, sunrise, sunset, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_id,
                data.get('district', 'Unknown'),
                data.get('date', datetime.now().strftime('%Y-%m-%d')),
                fetch_time,
                data.get('max_temp', 0.0),
                data.get('min_temp', 0.0),
                data.get('temp_range', 0.0),
                data.get('precipitation', 0.0),
                data.get('humidity', 0.0),
                data.get('wind_speed', 0.0),
                data.get('pressure', 0.0),
                int(data.get('cloudiness', 0)),
                data.get('visibility', 0.0),
                data.get('weather_desc', ''),
                data.get('sunrise', ''),
                data.get('sunset', ''),
                data.get('source', 'background_streamer')
            ))
            
            # Update counter for stats
            stat_date = datetime.now().strftime('%Y-%m-%d')
            hour = datetime.now().hour
            self.session.execute("""
                UPDATE streaming_stats 
                SET record_count = record_count + 1 
                WHERE stat_date = %s AND district = %s AND hour = %s
            """, (stat_date, data.get('district', 'Unknown'), hour))
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to store observation: {e}")
            return False
    
    def store_prediction(self, district: str, weather_data: Dict, 
                         heatwave_prob: float, flood_prob: float) -> bool:
        """Store a prediction."""
        if not self.connected:
            return False
        
        try:
            record_id = uuid.uuid4()
            
            def get_risk_level(prob: float) -> str:
                if prob >= 0.7:
                    return "HIGH"
                elif prob >= 0.4:
                    return "MEDIUM"
                return "LOW"
            
            self.session.execute("""
                INSERT INTO weather_predictions 
                (id, district, prediction_time, max_temp, precipitation, humidity,
                 heatwave_probability, flood_probability, heatwave_risk, flood_risk)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_id,
                district,
                datetime.now(),
                weather_data.get('max_temp', 0.0),
                weather_data.get('precipitation', 0.0),
                weather_data.get('humidity', 0.0),
                heatwave_prob,
                flood_prob,
                get_risk_level(heatwave_prob),
                get_risk_level(flood_prob)
            ))
            return True
            
        except Exception as e:
            print(f"❌ Failed to store prediction: {e}")
            return False
    
    def get_total_records(self) -> Dict[str, int]:
        """Get total record count per district."""
        counts = {}
        for district in DISTRICTS.keys():
            try:
                row = self.session.execute(
                    "SELECT COUNT(*) as cnt FROM weather_observations WHERE district = %s",
                    [district]
                ).one()
                counts[district] = row.cnt if row else 0
            except:
                counts[district] = 0
        return counts
    
    def close(self):
        """Close connection."""
        if self.cluster:
            self.cluster.shutdown()


class WeatherPredictor:
    """Handles ML predictions."""
    
    def __init__(self):
        self.heatwave_model = None
        self.flood_model = None
        self.models_loaded = False
        self._load_models()
    
    def _load_models(self):
        """Load XGBoost models."""
        try:
            import xgboost as xgb
            
            if os.path.exists('models/xgb_heatwave.json'):
                self.heatwave_model = xgb.Booster()
                self.heatwave_model.load_model('models/xgb_heatwave.json')
            
            if os.path.exists('models/xgb_flood_proxy.json'):
                self.flood_model = xgb.Booster()
                self.flood_model.load_model('models/xgb_flood_proxy.json')
            
            self.models_loaded = self.heatwave_model is not None and self.flood_model is not None
            
        except Exception as e:
            print(f"⚠️  Could not load models: {e}")
            self.models_loaded = False
    
    def predict(self, weather_data: Dict) -> tuple:
        """Make predictions from weather data."""
        if not self.models_loaded:
            return 0.0, 0.0
        
        try:
            import xgboost as xgb
            import numpy as np
            
            # Create feature array (matching training features)
            features = np.array([[
                weather_data.get('max_temp', 25.0),
                weather_data.get('min_temp', 15.0),
                weather_data.get('precipitation', 0.0),
                weather_data.get('humidity', 50.0),
                weather_data.get('wind_speed', 5.0),
                weather_data.get('pressure', 1013.0),
                weather_data.get('cloudiness', 50),
            ]])
            
            dmatrix = xgb.DMatrix(features)
            
            heatwave_prob = float(self.heatwave_model.predict(dmatrix)[0])
            flood_prob = float(self.flood_model.predict(dmatrix)[0])
            
            return heatwave_prob, flood_prob
            
        except Exception as e:
            return 0.0, 0.0


class DistrictStreamer:
    """Handles streaming for a single district."""
    
    def __init__(self, district: str, coords: Dict, api_key: str):
        self.district = district
        self.lat = coords['lat']
        self.lon = coords['lon']
        self.api_key = api_key
        self.stats = {
            'fetches': 0,
            'success': 0,
            'errors': 0,
            'last_fetch': None,
            'last_temp': None
        }
    
    def fetch_weather(self) -> Optional[Dict]:
        """Fetch weather from OpenWeatherMap."""
        if not self.api_key:
            return None
        
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': self.lat,
                'lon': self.lon,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Parse response
            weather_data = {
                'district': self.district,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'fetched_at': datetime.now().isoformat(),
                'max_temp': data['main'].get('temp_max', data['main']['temp']),
                'min_temp': data['main'].get('temp_min', data['main']['temp']),
                'temp_range': data['main'].get('temp_max', 0) - data['main'].get('temp_min', 0),
                'precipitation': data.get('rain', {}).get('1h', 0.0),
                'humidity': data['main'].get('humidity', 0),
                'wind_speed': data['wind'].get('speed', 0),
                'pressure': data['main'].get('pressure', 1013),
                'cloudiness': data['clouds'].get('all', 0),
                'visibility': data.get('visibility', 10000) / 1000,
                'weather_desc': data['weather'][0]['description'] if data.get('weather') else '',
                'sunrise': datetime.fromtimestamp(data['sys']['sunrise']).strftime('%H:%M') if data.get('sys', {}).get('sunrise') else '',
                'sunset': datetime.fromtimestamp(data['sys']['sunset']).strftime('%H:%M') if data.get('sys', {}).get('sunset') else '',
                'source': 'openweathermap'
            }
            
            self.stats['fetches'] += 1
            self.stats['success'] += 1
            self.stats['last_fetch'] = datetime.now().strftime('%H:%M:%S')
            self.stats['last_temp'] = weather_data['max_temp']
            
            return weather_data
            
        except Exception as e:
            self.stats['fetches'] += 1
            self.stats['errors'] += 1
            return None


class BackgroundStreamer:
    """Main background streaming service."""
    
    def __init__(self, interval: int = 30, districts: List[str] = None):
        self.interval = interval
        self.districts = districts or list(DISTRICTS.keys())
        self.running = False
        self.threads = {}
        
        # Components
        self.storage = CassandraStorage()
        self.predictor = WeatherPredictor()
        self.streamers: Dict[str, DistrictStreamer] = {}
        self.kafka_producer = None
        
        # Stats
        self.start_time = None
        self.total_records = 0
        
        # API key
        self.api_key = CONFIG['openweathermap_api_key']
    
    def setup(self) -> bool:
        """Initialize all components."""
        print("\n🔧 Setting up Background Streamer...")
        
        # Check API key
        if not self.api_key:
            print("❌ OpenWeatherMap API key not set!")
            print("   Set it with: $env:OPENWEATHERMAP_API_KEY='your_key'")
            return False
        print("✅ OpenWeatherMap API key configured")
        
        # Connect to Cassandra
        if self.storage.connect():
            print("✅ Connected to Cassandra")
        else:
            print("❌ Failed to connect to Cassandra")
            return False
        
        # Setup Kafka (optional)
        if KAFKA_AVAILABLE:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=CONFIG['kafka_bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("✅ Connected to Kafka")
            except:
                print("⚠️  Kafka not available (optional)")
        
        # Create district streamers
        for district in self.districts:
            if district in DISTRICTS:
                self.streamers[district] = DistrictStreamer(
                    district, DISTRICTS[district], self.api_key
                )
        print(f"✅ Initialized {len(self.streamers)} district streamers")
        
        # Check models
        if self.predictor.models_loaded:
            print("✅ ML models loaded")
        else:
            print("⚠️  ML models not available (predictions disabled)")
        
        return True
    
    def stream_district(self, district: str):
        """Stream data for a single district (runs in thread)."""
        streamer = self.streamers.get(district)
        if not streamer:
            return
        
        while self.running:
            try:
                # Fetch weather
                weather_data = streamer.fetch_weather()
                
                if weather_data:
                    # Store in Cassandra
                    if self.storage.store_observation(weather_data):
                        self.total_records += 1
                    
                    # Make predictions
                    if self.predictor.models_loaded:
                        heatwave_prob, flood_prob = self.predictor.predict(weather_data)
                        self.storage.store_prediction(
                            district, weather_data, heatwave_prob, flood_prob
                        )
                    
                    # Publish to Kafka (if available)
                    if self.kafka_producer:
                        try:
                            self.kafka_producer.send(
                                CONFIG['kafka_topic'],
                                weather_data
                            )
                        except:
                            pass
                
                # Wait for next fetch
                time.sleep(self.interval)
                
            except Exception as e:
                time.sleep(5)  # Brief pause on error
    
    def print_status(self):
        """Print current status."""
        while self.running:
            time.sleep(10)  # Update every 10 seconds
            
            if not self.running:
                break
            
            # Clear screen and print status
            os.system('cls' if os.name == 'nt' else 'clear')
            
            uptime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            hours, remainder = divmod(int(uptime), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            print("\n" + "="*70)
            print("  🌡️  BACKGROUND WEATHER STREAMER - LIVE STATUS")
            print("="*70)
            print(f"\n⏱️  Uptime: {hours:02d}:{minutes:02d}:{seconds:02d}")
            print(f"📊 Total Records Stored: {self.total_records}")
            print(f"⏰ Fetch Interval: {self.interval}s per district")
            print(f"🗄️  Cassandra: {'🟢 Connected' if self.storage.connected else '🔴 Disconnected'}")
            print(f"📡 Kafka: {'🟢 Connected' if self.kafka_producer else '⚪ Not used'}")
            
            print("\n" + "-"*70)
            print("  DISTRICT STATUS")
            print("-"*70)
            print(f"{'District':<12} {'Fetches':>8} {'Success':>8} {'Errors':>8} {'Last Fetch':>12} {'Temp':>8}")
            print("-"*70)
            
            for district, streamer in self.streamers.items():
                s = streamer.stats
                temp_str = f"{s['last_temp']:.1f}°C" if s['last_temp'] else "N/A"
                last_fetch = s['last_fetch'] or "Never"
                print(f"{district:<12} {s['fetches']:>8} {s['success']:>8} {s['errors']:>8} {last_fetch:>12} {temp_str:>8}")
            
            print("-"*70)
            
            # Get Cassandra counts
            counts = self.storage.get_total_records()
            total_cassandra = sum(counts.values())
            print(f"\n🗄️  Cassandra Storage: {total_cassandra} total records")
            for district, count in counts.items():
                bar = "█" * min(count // 10, 20)
                print(f"   {district:<12}: {count:>5} {bar}")
            
            print("\n" + "="*70)
            print("  Press Ctrl+C to stop streaming")
            print("="*70)
    
    def start(self):
        """Start background streaming."""
        if self.running:
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        print("\n🚀 Starting background streaming for all districts...")
        print(f"   Districts: {', '.join(self.districts)}")
        print(f"   Interval: {self.interval} seconds\n")
        
        # Start a thread for each district
        for district in self.streamers.keys():
            thread = threading.Thread(
                target=self.stream_district,
                args=(district,),
                daemon=True
            )
            thread.start()
            self.threads[district] = thread
            print(f"   ▶️  Started streaming for {district}")
        
        # Start status display thread
        status_thread = threading.Thread(target=self.print_status, daemon=True)
        status_thread.start()
        
        print("\n✅ All district streamers running!")
        print("   Data is being collected and stored in Cassandra.")
        print("   Press Ctrl+C to stop.\n")
    
    def stop(self):
        """Stop all streaming."""
        print("\n\n🛑 Stopping background streamer...")
        self.running = False
        
        # Wait for threads to finish
        time.sleep(2)
        
        # Close connections
        self.storage.close()
        if self.kafka_producer:
            self.kafka_producer.close()
        
        # Final stats
        uptime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        print(f"\n📊 Final Statistics:")
        print(f"   Total Records: {self.total_records}")
        print(f"   Uptime: {uptime:.0f} seconds")
        print(f"   Avg Rate: {self.total_records / max(uptime, 1) * 60:.1f} records/minute")
        print("\n✅ Streamer stopped.\n")


def main():
    parser = argparse.ArgumentParser(
        description="Background weather data streamer for all districts"
    )
    parser.add_argument(
        '--interval', '-i', type=int, default=30,
        help='Fetch interval in seconds (default: 30)'
    )
    parser.add_argument(
        '--districts', '-d', type=str,
        help='Comma-separated list of districts (default: all)'
    )
    
    args = parser.parse_args()
    
    # Parse districts
    districts = None
    if args.districts:
        districts = [d.strip() for d in args.districts.split(',')]
    
    # Print banner
    print("\n" + "="*70)
    print("  🌡️  BACKGROUND WEATHER DATA STREAMER")
    print("  Distributed Data Collection System")
    print("="*70)
    print("\n  Continuously fetches weather data from OpenWeatherMap")
    print("  for multiple districts and stores in Cassandra.")
    print("\n  This simulates a distributed data collection pipeline")
    print("  typical in Big Data systems.")
    print("="*70)
    
    # Create and setup streamer
    streamer = BackgroundStreamer(interval=args.interval, districts=districts)
    
    if not streamer.setup():
        print("\n❌ Setup failed. Please check configuration.")
        sys.exit(1)
    
    # Handle Ctrl+C
    def signal_handler(sig, frame):
        streamer.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start streaming
    streamer.start()
    
    # Keep main thread alive
    try:
        while streamer.running:
            time.sleep(1)
    except KeyboardInterrupt:
        streamer.stop()


if __name__ == "__main__":
    main()
