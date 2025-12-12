"""
Gradio Dashboard with Kafka Consumer + Cassandra Storage
Subscribes to Kafka topic, stores data in Cassandra, and displays real-time predictions.

Usage:
    python kafka_consumer_gradio.py

Architecture:
    API → Kafka Producer → Kafka Topic → Kafka Consumer → Cassandra → Gradio Dashboard
                              ↓
                      'weather-data' topic
"""

import os
import json
import time
import threading
import uuid
from datetime import datetime
from queue import Queue, Empty
from collections import deque
import warnings
warnings.filterwarnings('ignore')

import gradio as gr
import pandas as pd
import numpy as np

# Try importing Kafka
try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️  kafka-python not installed. Run: pip install kafka-python")

# Try importing Cassandra
try:
    from cassandra.cluster import Cluster
    from cassandra.query import BatchStatement, ConsistencyLevel
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    print("⚠️  cassandra-driver not installed. Run: pip install cassandra-driver")

# Try importing ML libraries
try:
    import joblib
    import xgboost as xgb
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False


# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic': 'weather-data',
    'group_id': 'gradio-consumer-group',
    'auto_offset_reset': 'latest'
}

CASSANDRA_CONFIG = {
    'hosts': ['127.0.0.1'],
    'port': 9042,
    'keyspace': 'weather_stream'
}

MODEL_PATHS = {
    'heatwave': 'models/xgb_heatwave_model.joblib',
    'flood': 'models/xgb_flood_proxy_model.joblib'
}


# ============================================================================
# CASSANDRA STORAGE
# ============================================================================

class CassandraStorage:
    """Cassandra storage for streaming weather data and predictions."""
    
    def __init__(self, hosts: list, port: int, keyspace: str):
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        self.connected = False
        self.prepared_statements = {}
        self.stats = {
            'records_stored': 0,
            'predictions_stored': 0,
            'errors': 0
        }
    
    def connect(self) -> bool:
        """Connect to Cassandra cluster."""
        if not CASSANDRA_AVAILABLE:
            print("⚠️  Cassandra driver not available")
            return False
        
        try:
            self.cluster = Cluster(contact_points=self.hosts, port=self.port)
            self.session = self.cluster.connect()
            self.connected = True
            print(f"✅ Connected to Cassandra at {self.hosts}:{self.port}")
            self._setup_schema()
            self._prepare_statements()
            return True
        except Exception as e:
            print(f"⚠️  Cassandra connection failed: {e}")
            self.connected = False
            return False
    
    def _setup_schema(self):
        """Create keyspace and tables if they don't exist."""
        # Create keyspace
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        
        self.session.set_keyspace(self.keyspace)
        
        # Table for raw streaming data
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS streaming_weather (
                id UUID,
                district TEXT,
                date TEXT,
                ingestion_time TIMESTAMP,
                latitude DOUBLE,
                longitude DOUBLE,
                max_temp DOUBLE,
                min_temp DOUBLE,
                temp_range DOUBLE,
                precipitation DOUBLE,
                humidity DOUBLE,
                wind_speed_10m DOUBLE,
                wind_speed_50m DOUBLE,
                kafka_partition INT,
                kafka_offset BIGINT,
                PRIMARY KEY ((district), ingestion_time, id)
            ) WITH CLUSTERING ORDER BY (ingestion_time DESC)
        """)
        
        # Table for predictions
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS streaming_predictions (
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
        
        # Table for aggregated stats (per district per hour)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS hourly_stats (
                district TEXT,
                hour_bucket TIMESTAMP,
                record_count INT,
                avg_temp DOUBLE,
                max_temp DOUBLE,
                total_precip DOUBLE,
                heatwave_alerts INT,
                flood_alerts INT,
                PRIMARY KEY ((district), hour_bucket)
            ) WITH CLUSTERING ORDER BY (hour_bucket DESC)
        """)
        
        print("✅ Cassandra schema ready")
    
    def _prepare_statements(self):
        """Prepare CQL statements for better performance."""
        self.prepared_statements['insert_weather'] = self.session.prepare("""
            INSERT INTO streaming_weather 
            (id, district, date, ingestion_time, latitude, longitude, max_temp, min_temp, 
             temp_range, precipitation, humidity, wind_speed_10m, wind_speed_50m,
             kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        self.prepared_statements['insert_prediction'] = self.session.prepare("""
            INSERT INTO streaming_predictions
            (id, district, prediction_time, max_temp, precipitation, humidity,
             heatwave_probability, flood_probability, heatwave_risk, flood_risk)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        print("✅ Cassandra prepared statements ready")
    
    def store_weather_data(self, data: dict) -> bool:
        """Store raw weather data from Kafka."""
        if not self.connected:
            return False
        
        try:
            self.session.execute(
                self.prepared_statements['insert_weather'],
                (
                    uuid.uuid4(),
                    data.get('District', 'Unknown'),
                    data.get('Date', ''),
                    datetime.now(),
                    float(data.get('Latitude', 0)),
                    float(data.get('Longitude', 0)),
                    float(data.get('MaxTemp_2m', 0)),
                    float(data.get('MinTemp_2m', 0)),
                    float(data.get('TempRange_2m', 0)),
                    float(data.get('Precip', 0)),
                    float(data.get('RH_2m', 0)),
                    float(data.get('WindSpeed_10m', 0)),
                    float(data.get('WindSpeed_50m', 0)),
                    int(data.get('kafka_partition', 0)),
                    int(data.get('kafka_offset', 0))
                )
            )
            self.stats['records_stored'] += 1
            return True
        except Exception as e:
            print(f"❌ Error storing weather data: {e}")
            self.stats['errors'] += 1
            return False
    
    def store_prediction(self, data: dict, prediction: dict) -> bool:
        """Store prediction results."""
        if not self.connected:
            return False
        
        try:
            self.session.execute(
                self.prepared_statements['insert_prediction'],
                (
                    uuid.uuid4(),
                    data.get('District', 'Unknown'),
                    datetime.now(),
                    float(data.get('MaxTemp_2m', 0)),
                    float(data.get('Precip', 0)),
                    float(data.get('RH_2m', 0)),
                    float(prediction.get('heatwave_probability', 0)),
                    float(prediction.get('flood_probability', 0)),
                    prediction.get('heatwave_risk', 'LOW'),
                    prediction.get('flood_risk', 'LOW')
                )
            )
            self.stats['predictions_stored'] += 1
            return True
        except Exception as e:
            print(f"❌ Error storing prediction: {e}")
            self.stats['errors'] += 1
            return False
    
    def get_recent_data(self, district: str = None, limit: int = 20) -> list:
        """Query recent streaming data."""
        if not self.connected:
            return []
        
        try:
            if district:
                rows = self.session.execute(f"""
                    SELECT * FROM streaming_weather 
                    WHERE district = '{district}'
                    LIMIT {limit}
                """)
            else:
                # Get from all districts (limited query)
                rows = self.session.execute(f"""
                    SELECT * FROM streaming_weather 
                    LIMIT {limit}
                    ALLOW FILTERING
                """)
            return list(rows)
        except Exception as e:
            print(f"❌ Query error: {e}")
            return []
    
    def get_recent_predictions(self, district: str = None, limit: int = 20) -> list:
        """Query recent predictions."""
        if not self.connected:
            return []
        
        try:
            if district:
                rows = self.session.execute(f"""
                    SELECT * FROM streaming_predictions 
                    WHERE district = '{district}'
                    LIMIT {limit}
                """)
            else:
                rows = self.session.execute(f"""
                    SELECT * FROM streaming_predictions 
                    LIMIT {limit}
                    ALLOW FILTERING
                """)
            return list(rows)
        except Exception as e:
            print(f"❌ Query error: {e}")
            return []
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        return self.stats.copy()
    
    def close(self):
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            self.connected = False


# ============================================================================
# PREDICTION ENGINE
# ============================================================================

class PredictionEngine:
    """Loads models and makes predictions."""
    
    def __init__(self):
        self.heatwave_model = None
        self.flood_model = None
        self.load_models()
    
    def load_models(self):
        """Load XGBoost models."""
        if not ML_AVAILABLE:
            print("⚠️  ML libraries not available")
            return
        
        try:
            if os.path.exists(MODEL_PATHS['heatwave']):
                self.heatwave_model = joblib.load(MODEL_PATHS['heatwave'])
                print(f"✅ Loaded heatwave model")
            
            if os.path.exists(MODEL_PATHS['flood']):
                self.flood_model = joblib.load(MODEL_PATHS['flood'])
                print(f"✅ Loaded flood model")
        except Exception as e:
            print(f"❌ Error loading models: {e}")
    
    def predict(self, data: dict) -> dict:
        """Make predictions from weather data."""
        # Extract features
        features = self._extract_features(data)
        
        heatwave_prob = 0.0
        flood_prob = 0.0
        
        # Heatwave prediction
        if self.heatwave_model is not None:
            try:
                if isinstance(self.heatwave_model, xgb.Booster):
                    dmatrix = xgb.DMatrix([list(features.values())], feature_names=list(features.keys()))
                    heatwave_prob = float(self.heatwave_model.predict(dmatrix)[0])
                else:
                    heatwave_prob = float(self.heatwave_model.predict_proba([list(features.values())])[0][1])
            except Exception as e:
                # Simple rule-based fallback
                heatwave_prob = min(1.0, max(0.0, (data.get('MaxTemp_2m', 30) - 35) / 15))
        else:
            # Rule-based prediction
            temp = data.get('MaxTemp_2m', 30)
            heatwave_prob = min(1.0, max(0.0, (temp - 35) / 15))
        
        # Flood prediction
        if self.flood_model is not None:
            try:
                if isinstance(self.flood_model, xgb.Booster):
                    dmatrix = xgb.DMatrix([list(features.values())], feature_names=list(features.keys()))
                    flood_prob = float(self.flood_model.predict(dmatrix)[0])
                else:
                    flood_prob = float(self.flood_model.predict_proba([list(features.values())])[0][1])
            except Exception as e:
                # Simple rule-based fallback
                flood_prob = min(1.0, max(0.0, (data.get('Precip', 0) - 50) / 100))
        else:
            # Rule-based prediction
            precip = data.get('Precip', 0)
            humidity = data.get('RH_2m', 50)
            flood_prob = min(1.0, max(0.0, (precip - 50) / 100 + (humidity - 80) / 100))
        
        return {
            'heatwave_probability': heatwave_prob,
            'flood_probability': flood_prob,
            'heatwave_risk': 'HIGH' if heatwave_prob > 0.5 else 'MEDIUM' if heatwave_prob > 0.3 else 'LOW',
            'flood_risk': 'HIGH' if flood_prob > 0.5 else 'MEDIUM' if flood_prob > 0.3 else 'LOW'
        }
    
    def _extract_features(self, data: dict) -> dict:
        """Extract model features from raw data."""
        # Basic features (simplified - in production, compute rolling features)
        return {
            'Precip': data.get('Precip', 0),
            'precip_3d': data.get('Precip', 0) * 3,  # Simplified
            'precip_7d': data.get('Precip', 0) * 7,
            'precip_lag_1': data.get('Precip', 0),
            'precip_lag_3': data.get('Precip', 0),
            'precip_lag_7': data.get('Precip', 0),
            'MaxTemp_2m': data.get('MaxTemp_2m', 30),
            'maxT_3d_mean': data.get('MaxTemp_2m', 30),
            'maxT_lag_1': data.get('MaxTemp_2m', 30),
            'maxT_lag_3': data.get('MaxTemp_2m', 30),
            'anom_maxT': data.get('MaxTemp_2m', 30) - 30,
            'RH_2m': data.get('RH_2m', 50),
            'wetness_flag': 1 if data.get('RH_2m', 50) > 80 else 0,
            'API': data.get('Precip', 0) * 0.9,
            'TempRange_2m': data.get('TempRange_2m', 10),
            'WindSpeed_10m': data.get('WindSpeed_10m', 5),
            'WindSpeed_50m': data.get('WindSpeed_50m', 10),
            'doy_sin': 0.5,
            'doy_cos': 0.5,
            'month': datetime.now().month,
            'year': datetime.now().year
        }


# ============================================================================
# KAFKA CONSUMER
# ============================================================================

class WeatherKafkaConsumer:
    """Kafka consumer that subscribes to weather data topic and stores in Cassandra."""
    
    def __init__(self, bootstrap_servers: list, topic: str, group_id: str, cassandra_storage=None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.message_queue = Queue(maxsize=1000)
        self.recent_messages = deque(maxlen=100)
        self.stats = {
            'messages_received': 0,
            'messages_stored': 0,
            'predictions_stored': 0,
            'errors': 0,
            'last_message_time': None
        }
        self.consumer_thread = None
        self.prediction_engine = PredictionEngine()
        self.cassandra = cassandra_storage  # Cassandra storage instance
    
    def connect(self) -> bool:
        """Connect to Kafka broker."""
        if not KAFKA_AVAILABLE:
            print("⚠️  Kafka not available, running in simulation mode")
            return False
        
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=KAFKA_CONFIG['auto_offset_reset'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            print(f"✅ Connected to Kafka, subscribed to '{self.topic}'")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def start_consuming(self):
        """Start consuming messages in background thread."""
        if not self.consumer:
            if not self.connect():
                return
        
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.consumer_thread.start()
        print("🔄 Started Kafka consumer thread")
    
    def _consume_loop(self):
        """Background loop to consume messages."""
        while self.running:
            try:
                # Poll for messages
                for message in self.consumer:
                    if not self.running:
                        break
                    
                    data = message.value
                    data['kafka_partition'] = message.partition
                    data['kafka_offset'] = message.offset
                    
                    # Make prediction
                    prediction = self.prediction_engine.predict(data)
                    
                    # Store in Cassandra
                    if self.cassandra and self.cassandra.connected:
                        if self.cassandra.store_weather_data(data):
                            self.stats['messages_stored'] += 1
                        if self.cassandra.store_prediction(data, prediction):
                            self.stats['predictions_stored'] += 1
                    
                    # Combine data with prediction
                    enriched = {
                        **data,
                        **prediction,
                        'consumed_at': datetime.now().isoformat()
                    }
                    
                    # Add to queue and recent messages
                    self.message_queue.put(enriched)
                    self.recent_messages.append(enriched)
                    
                    self.stats['messages_received'] += 1
                    self.stats['last_message_time'] = datetime.now().isoformat()
                    
            except Exception as e:
                if self.running:
                    self.stats['errors'] += 1
                    time.sleep(1)  # Wait before retry
    
    def get_message(self, timeout: float = 0.1):
        """Get a message from the queue."""
        try:
            return self.message_queue.get(timeout=timeout)
        except Empty:
            return None
    
    def get_recent_messages(self, n: int = 20) -> list:
        """Get recent messages."""
        return list(self.recent_messages)[-n:]
    
    def get_stats(self) -> dict:
        """Get consumer statistics."""
        return self.stats.copy()
    
    def stop(self):
        """Stop consuming."""
        self.running = False
        if self.consumer:
            self.consumer.close()


# ============================================================================
# GRADIO DASHBOARD
# ============================================================================

# Global Cassandra storage instance
cassandra_storage = CassandraStorage(
    hosts=CASSANDRA_CONFIG['hosts'],
    port=CASSANDRA_CONFIG['port'],
    keyspace=CASSANDRA_CONFIG['keyspace']
)

# Global consumer instance with Cassandra
consumer = WeatherKafkaConsumer(
    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
    topic=KAFKA_CONFIG['topic'],
    group_id=KAFKA_CONFIG['group_id'],
    cassandra_storage=cassandra_storage
)


def create_stats_html(stats: dict, latest: dict = None, cassandra_stats: dict = None) -> str:
    """Create statistics display HTML."""
    heatwave_prob = latest.get('heatwave_probability', 0) * 100 if latest else 0
    flood_prob = latest.get('flood_probability', 0) * 100 if latest else 0
    
    heatwave_color = "#ef4444" if heatwave_prob > 50 else "#f59e0b" if heatwave_prob > 30 else "#22c55e"
    flood_color = "#3b82f6" if flood_prob > 50 else "#06b6d4" if flood_prob > 30 else "#22c55e"
    
    cassandra_stored = cassandra_stats.get('records_stored', 0) if cassandra_stats else 0
    
    return f"""
    <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; padding: 15px;">
        <div style="background: linear-gradient(135deg, #1e40af, #3b82f6); padding: 18px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 13px; color: #93c5fd;">📨 Kafka Messages</div>
            <div style="font-size: 28px; font-weight: bold; color: white;">{stats.get('messages_received', 0):,}</div>
        </div>
        <div style="background: linear-gradient(135deg, #7c2d12, {heatwave_color}); padding: 18px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 13px; color: #fecaca;">🔥 Heatwave Risk</div>
            <div style="font-size: 28px; font-weight: bold; color: white;">{heatwave_prob:.1f}%</div>
        </div>
        <div style="background: linear-gradient(135deg, #1e3a5f, {flood_color}); padding: 18px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 13px; color: #bfdbfe;">🌊 Flood Risk</div>
            <div style="font-size: 28px; font-weight: bold; color: white;">{flood_prob:.1f}%</div>
        </div>
        <div style="background: linear-gradient(135deg, #581c87, #9333ea); padding: 18px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 13px; color: #e9d5ff;">🗄️ Cassandra Stored</div>
            <div style="font-size: 28px; font-weight: bold; color: white;">{cassandra_stored:,}</div>
        </div>
        <div style="background: linear-gradient(135deg, #14532d, #22c55e); padding: 18px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 13px; color: #bbf7d0;">📍 Latest District</div>
            <div style="font-size: 22px; font-weight: bold; color: white;">{latest.get('District', 'N/A') if latest else 'Waiting...'}</div>
        </div>
    </div>
    """


def create_latest_data_html(data: dict) -> str:
    """Create HTML for latest weather data."""
    if not data:
        return """
        <div style="text-align: center; padding: 40px; color: #64748b;">
            <div style="font-size: 48px;">📡</div>
            <div style="font-size: 18px; margin-top: 10px;">Waiting for data from Kafka...</div>
            <div style="font-size: 14px; margin-top: 5px;">Send data via POST /weather API</div>
        </div>
        """
    
    temp = data.get('MaxTemp_2m', 0)
    precip = data.get('Precip', 0)
    humidity = data.get('RH_2m', 0)
    
    temp_color = "#ef4444" if temp > 40 else "#f59e0b" if temp > 35 else "#22c55e"
    precip_color = "#3b82f6" if precip > 50 else "#06b6d4" if precip > 10 else "#64748b"
    
    return f"""
    <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 20px; 
                border-radius: 16px; border: 1px solid #475569;">
        <h3 style="color: #f1f5f9; margin-bottom: 15px;">📊 Latest Weather Data</h3>
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px;">
            <div style="text-align: center;">
                <div style="font-size: 36px; color: {temp_color}; font-weight: bold;">{temp:.1f}°C</div>
                <div style="color: #94a3b8;">🌡️ Temperature</div>
            </div>
            <div style="text-align: center;">
                <div style="font-size: 36px; color: {precip_color}; font-weight: bold;">{precip:.1f}mm</div>
                <div style="color: #94a3b8;">🌧️ Precipitation</div>
            </div>
            <div style="text-align: center;">
                <div style="font-size: 36px; color: #06b6d4; font-weight: bold;">{humidity:.1f}%</div>
                <div style="color: #94a3b8;">💧 Humidity</div>
            </div>
        </div>
        <div style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #475569; color: #94a3b8; font-size: 12px;">
            📍 {data.get('District', 'Unknown')} | 📅 {data.get('Date', 'N/A')} | ⏱️ {data.get('consumed_at', 'N/A')[:19]}
        </div>
    </div>
    """


def create_message_table(messages: list) -> pd.DataFrame:
    """Create DataFrame from recent messages."""
    if not messages:
        return pd.DataFrame(columns=['Time', 'District', 'Temp (°C)', 'Precip (mm)', 'Heatwave %', 'Flood %'])
    
    rows = []
    for msg in reversed(messages[-15:]):  # Last 15, newest first
        rows.append({
            'Time': msg.get('consumed_at', '')[:19] if msg.get('consumed_at') else '',
            'District': msg.get('District', ''),
            'Temp (°C)': round(msg.get('MaxTemp_2m', 0), 1),
            'Precip (mm)': round(msg.get('Precip', 0), 1),
            'Heatwave %': round(msg.get('heatwave_probability', 0) * 100, 1),
            'Flood %': round(msg.get('flood_probability', 0) * 100, 1)
        })
    
    return pd.DataFrame(rows)


def refresh_dashboard():
    """Refresh dashboard with latest data."""
    stats = consumer.get_stats()
    messages = consumer.get_recent_messages(20)
    latest = messages[-1] if messages else None
    
    # Cassandra stats
    cassandra_stats = {
        'records_stored': stats.get('messages_stored', 0),
        'predictions_stored': stats.get('predictions_stored', 0)
    }
    
    return (
        create_stats_html(stats, latest, cassandra_stats),
        create_latest_data_html(latest),
        create_message_table(messages)
    )


def start_consumer():
    """Start the Kafka consumer."""
    consumer.start_consuming()
    return "✅ Consumer started! Listening for messages..."


def stop_consumer():
    """Stop the Kafka consumer."""
    consumer.stop()
    return "⏹️ Consumer stopped"


# Custom CSS
CUSTOM_CSS = """
.gradio-container {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%) !important;
}
.main-title {
    text-align: center;
    font-size: 2.2em;
    font-weight: 700;
    background: linear-gradient(135deg, #3b82f6, #22c55e);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    padding: 15px;
}
.section-header {
    color: #94a3b8;
    font-size: 1.1em;
    font-weight: 600;
    border-bottom: 2px solid #3b82f6;
    padding-bottom: 8px;
    margin: 15px 0 10px 0;
}
"""


def create_dashboard():
    """Create the Gradio dashboard."""
    
    with gr.Blocks(
        title="🌊 Kafka Weather Consumer Dashboard",
        theme=gr.themes.Soft(
            primary_hue="blue",
            secondary_hue="green",
            neutral_hue="slate"
        ),
        css=CUSTOM_CSS
    ) as app:
        
        # Header
        gr.HTML("""
        <div class="main-title">
            🌡️ Real-Time Weather Prediction Dashboard 🌊
        </div>
        <div style="text-align: center; color: #64748b; margin-bottom: 15px;">
            Kafka Consumer → ML Prediction → Live Dashboard
        </div>
        """)
        
        # Control buttons
        with gr.Row():
            start_btn = gr.Button("▶️ Start Consumer", variant="primary", size="lg")
            refresh_btn = gr.Button("🔄 Refresh", variant="secondary", size="lg")
            stop_btn = gr.Button("⏹️ Stop Consumer", variant="stop", size="lg")
        
        status_text = gr.Textbox(label="Status", value="Click 'Start Consumer' to begin", interactive=False)
        
        # Statistics
        gr.HTML('<div class="section-header">📊 Real-Time Statistics & Predictions</div>')
        stats_html = gr.HTML(value=create_stats_html({}, None, None))
        
        # Latest Data
        gr.HTML('<div class="section-header">📡 Latest Weather Data</div>')
        latest_html = gr.HTML(value=create_latest_data_html(None))
        
        # Message History
        gr.HTML('<div class="section-header">📋 Recent Messages</div>')
        message_table = gr.Dataframe(
            value=create_message_table([]),
            interactive=False,
            wrap=True
        )
        
        # Auto-refresh timer
        timer = gr.Timer(value=2)  # Refresh every 2 seconds
        
        # Info
        with gr.Accordion("ℹ️ Architecture & Usage", open=False):
            gr.Markdown("""
            ## Kafka + Cassandra Streaming Architecture
            
            ```
            ┌──────────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
            │   Weather    │     │   Kafka     │     │   Kafka      │     │   Gradio    │
            │   API        │────▶│   Producer  │────▶│   Topic      │────▶│   Consumer  │
            │   (FastAPI)  │     │             │     │              │     │   Dashboard │
            └──────────────┘     └─────────────┘     └──────────────┘     └─────────────┘
                  ↑                                                              │
              HTTP POST                                                    ML Prediction
              /weather                                                          │
                                                                                ▼
                                                                  ┌─────────────────────────┐
                                                                  │   Apache Cassandra      │
                                                                  │   - streaming_weather   │
                                                                  │   - streaming_predictions│
                                                                  │   - hourly_stats        │
                                                                  └─────────────────────────┘
            ```
            
            ## Data Flow
            
            1. **API** receives weather data via REST
            2. **Kafka** handles message streaming
            3. **Consumer** makes ML predictions (XGBoost + LSTM)
            4. **Cassandra** stores raw data + predictions for time-series analysis
            5. **Dashboard** displays real-time statistics
            
            ## How to Use
            
            1. **Start services**: `docker-compose up -d` (Kafka + Cassandra)
            2. **Start Producer API**: `python kafka_producer_api.py`
            3. **Start this Dashboard**: `python kafka_consumer_gradio.py`
            4. **Send data**: POST to `http://localhost:8000/weather`
            
            ## Sample API Request
            
            ```bash
            curl -X POST http://localhost:8000/weather \\
              -H "Content-Type: application/json" \\
              -d '{"district": "Chitawan", "date": "2024-06-15", "max_temp": 42.5, "precipitation": 15.0, "humidity": 75.0}'
            ```
            
            ## Cassandra Tables
            
            - **streaming_weather**: Raw weather observations (partition by district, cluster by time)
            - **streaming_predictions**: ML predictions (partition by prediction_type, cluster by time)
            - **hourly_stats**: Aggregated hourly statistics
            """)
        
        # Event handlers
        start_btn.click(fn=start_consumer, outputs=[status_text])
        stop_btn.click(fn=stop_consumer, outputs=[status_text])
        refresh_btn.click(fn=refresh_dashboard, outputs=[stats_html, latest_html, message_table])
        timer.tick(fn=refresh_dashboard, outputs=[stats_html, latest_html, message_table])
    
    return app


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   🌡️  KAFKA WEATHER CONSUMER - GRADIO DASHBOARD                              ║
║                                                                              ║
║   Subscribes to Kafka and stores data in Cassandra with real-time predictions║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📡 Kafka Configuration:
   Topic: {topic}
   Servers: {kafka_servers}
   Group ID: {group}

🗄️ Cassandra Configuration:
   Hosts: {cassandra_hosts}
   Port: {cassandra_port}
   Keyspace: {keyspace}
   Tables: streaming_weather, streaming_predictions, hourly_stats

🔗 Dashboard: http://localhost:7861

💡 Prerequisites:
   1. Start Kafka + Cassandra: docker-compose up -d
   2. Start Producer API: python kafka_producer_api.py
   3. Send data to API: POST http://localhost:8000/weather
""".format(
        topic=KAFKA_CONFIG['topic'],
        kafka_servers=KAFKA_CONFIG['bootstrap_servers'],
        group=KAFKA_CONFIG['group_id'],
        cassandra_hosts=CASSANDRA_CONFIG['hosts'],
        cassandra_port=CASSANDRA_CONFIG['port'],
        keyspace=CASSANDRA_CONFIG['keyspace']
    ))
    
    app = create_dashboard()
    app.launch(
        server_name="127.0.0.1",
        server_port=7861,
        share=False
    )


if __name__ == "__main__":
    main()
