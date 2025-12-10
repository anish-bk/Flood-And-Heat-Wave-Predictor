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

MODEL_PATHS = {
    'heatwave': 'models/xgb_heatwave_model.joblib',
    'flood': 'models/xgb_flood_proxy_model.joblib'
}


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
    """Kafka consumer that subscribes to weather data topic."""
    
    def __init__(self, bootstrap_servers: list, topic: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.message_queue = Queue(maxsize=1000)
        self.recent_messages = deque(maxlen=100)
        self.stats = {
            'messages_received': 0,
            'errors': 0,
            'last_message_time': None
        }
        self.consumer_thread = None
        self.prediction_engine = PredictionEngine()
    
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
                    
                    # Make prediction
                    prediction = self.prediction_engine.predict(data)
                    
                    # Combine data with prediction
                    enriched = {
                        **data,
                        **prediction,
                        'kafka_partition': message.partition,
                        'kafka_offset': message.offset,
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

# Global consumer instance
consumer = WeatherKafkaConsumer(
    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
    topic=KAFKA_CONFIG['topic'],
    group_id=KAFKA_CONFIG['group_id']
)


def create_stats_html(stats: dict, latest: dict = None) -> str:
    """Create statistics display HTML."""
    heatwave_prob = latest.get('heatwave_probability', 0) * 100 if latest else 0
    flood_prob = latest.get('flood_probability', 0) * 100 if latest else 0
    
    heatwave_color = "#ef4444" if heatwave_prob > 50 else "#f59e0b" if heatwave_prob > 30 else "#22c55e"
    flood_color = "#3b82f6" if flood_prob > 50 else "#06b6d4" if flood_prob > 30 else "#22c55e"
    
    return f"""
    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; padding: 15px;">
        <div style="background: linear-gradient(135deg, #1e40af, #3b82f6); padding: 20px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 14px; color: #93c5fd;">📨 Messages Received</div>
            <div style="font-size: 32px; font-weight: bold; color: white;">{stats.get('messages_received', 0):,}</div>
        </div>
        <div style="background: linear-gradient(135deg, #7c2d12, {heatwave_color}); padding: 20px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 14px; color: #fecaca;">🔥 Heatwave Risk</div>
            <div style="font-size: 32px; font-weight: bold; color: white;">{heatwave_prob:.1f}%</div>
        </div>
        <div style="background: linear-gradient(135deg, #1e3a5f, {flood_color}); padding: 20px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 14px; color: #bfdbfe;">🌊 Flood Risk</div>
            <div style="font-size: 32px; font-weight: bold; color: white;">{flood_prob:.1f}%</div>
        </div>
        <div style="background: linear-gradient(135deg, #14532d, #22c55e); padding: 20px; 
                    border-radius: 16px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
            <div style="font-size: 14px; color: #bbf7d0;">📍 Latest District</div>
            <div style="font-size: 24px; font-weight: bold; color: white;">{latest.get('District', 'N/A') if latest else 'Waiting...'}</div>
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
    
    return (
        create_stats_html(stats, latest),
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
        stats_html = gr.HTML(value=create_stats_html({}, None))
        
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
            ## Kafka Streaming Architecture
            
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
                                                                        Real-Time Display
            ```
            
            ## How to Use
            
            1. **Start Kafka**: `docker-compose up -d`
            2. **Start Producer API**: `python kafka_producer_api.py`
            3. **Start this Dashboard**: `python kafka_consumer_gradio.py`
            4. **Send data**: POST to `http://localhost:8000/weather`
            
            ## Sample API Request
            
            ```bash
            curl -X POST http://localhost:8000/weather \\
              -H "Content-Type: application/json" \\
              -d '{"district": "Chitawan", "date": "2024-06-15", "max_temp": 42.5, "precipitation": 15.0, "humidity": 75.0}'
            ```
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
║   Subscribes to Kafka and displays real-time weather predictions             ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📡 Kafka Configuration:
   Topic: {topic}
   Servers: {servers}
   Group ID: {group}

🔗 Dashboard: http://localhost:7860

💡 Prerequisites:
   1. Start Kafka: docker-compose up -d
   2. Start Producer API: python kafka_producer_api.py
   3. Send data to API: POST http://localhost:8000/weather
""".format(
        topic=KAFKA_CONFIG['topic'],
        servers=KAFKA_CONFIG['bootstrap_servers'],
        group=KAFKA_CONFIG['group_id']
    ))
    
    app = create_dashboard()
    app.launch(
        server_name="127.0.0.1",
        server_port=7860,
        share=False
    )


if __name__ == "__main__":
    main()
