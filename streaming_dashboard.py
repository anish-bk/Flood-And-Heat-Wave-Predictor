"""
Live Streaming Dashboard with Gradio
Real-time visualization of weather data streaming from distributed sources.

Run with: python streaming_dashboard.py
"""

import os
import time
import random
from datetime import datetime
import threading
import gradio as gr
import pandas as pd
import numpy as np

# Data directory
DATA_DIR = "data/district_files"


class StreamingEngine:
    """Backend engine for streaming simulation."""
    
    def __init__(self):
        self.stations = {}
        self.current_indices = {}
        self.is_streaming = False
        self.stats = {
            'total_records': 0,
            'heatwave_alerts': 0,
            'flood_alerts': 0,
            'start_time': None
        }
        self.recent_records = []
        self.load_data()
    
    def load_data(self):
        """Load all station data."""
        if os.path.exists(DATA_DIR):
            for f in sorted(os.listdir(DATA_DIR)):
                if f.endswith('.csv'):
                    district = f.replace('.csv', '')
                    df = pd.read_csv(os.path.join(DATA_DIR, f), parse_dates=['Date'])
                    df = df.sort_values('Date').reset_index(drop=True)
                    self.stations[district] = df
                    self.current_indices[district] = 0
    
    def reset(self):
        """Reset streaming state."""
        for district in self.current_indices:
            self.current_indices[district] = 0
        self.stats = {
            'total_records': 0,
            'heatwave_alerts': 0,
            'flood_alerts': 0,
            'start_time': datetime.now()
        }
        self.recent_records = []
    
    def get_next_batch(self, batch_size=10):
        """Get next batch of records from all stations (round-robin)."""
        records = []
        
        for district, df in self.stations.items():
            idx = self.current_indices[district]
            if idx < len(df):
                record = df.iloc[idx].to_dict()
                record['District'] = district
                record['StreamTime'] = datetime.now().strftime('%H:%M:%S')
                records.append(record)
                self.current_indices[district] = idx + 1
                self.stats['total_records'] += 1
                
                # Check alerts
                if record.get('MaxTemp_2m', 0) > 40:
                    self.stats['heatwave_alerts'] += 1
                if record.get('Precip', 0) > 100:
                    self.stats['flood_alerts'] += 1
        
        # Keep only recent records for display
        self.recent_records = (self.recent_records + records)[-100:]
        
        return records
    
    def get_summary_stats(self):
        """Get summary statistics."""
        if not self.recent_records:
            return {}
        
        df = pd.DataFrame(self.recent_records)
        return {
            'avg_temp': df['MaxTemp_2m'].mean() if 'MaxTemp_2m' in df.columns else 0,
            'max_temp': df['MaxTemp_2m'].max() if 'MaxTemp_2m' in df.columns else 0,
            'total_precip': df['Precip'].sum() if 'Precip' in df.columns else 0,
            'records': len(self.recent_records),
            'districts': df['District'].nunique()
        }


# Global engine instance
engine = StreamingEngine()


def create_station_status_html():
    """Create HTML showing station status."""
    html = """
    <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px; padding: 10px;">
    """
    
    for district, df in engine.stations.items():
        idx = engine.current_indices[district]
        total = len(df)
        progress = (idx / total) * 100
        
        # Color based on progress
        if progress > 80:
            color = "#ef4444"  # red
        elif progress > 50:
            color = "#f59e0b"  # yellow
        else:
            color = "#22c55e"  # green
        
        html += f"""
        <div style="background: linear-gradient(135deg, #1e293b, #334155); 
                    padding: 12px; border-radius: 12px; text-align: center;
                    border: 1px solid {color}40;">
            <div style="font-size: 12px; color: #94a3b8;">📡 {district}</div>
            <div style="font-size: 18px; font-weight: bold; color: {color};">{idx:,}</div>
            <div style="font-size: 10px; color: #64748b;">/ {total:,}</div>
            <div style="background: #1e293b; border-radius: 4px; height: 6px; margin-top: 8px;">
                <div style="background: {color}; height: 6px; border-radius: 4px; width: {progress:.1f}%;"></div>
            </div>
        </div>
        """
    
    html += "</div>"
    return html


def create_stats_html():
    """Create statistics display HTML."""
    stats = engine.get_summary_stats()
    total = engine.stats
    
    avg_temp = stats.get('avg_temp', 0)
    max_temp = stats.get('max_temp', 0)
    total_precip = stats.get('total_precip', 0)
    
    # Temperature color
    temp_color = "#ef4444" if max_temp > 40 else "#f59e0b" if max_temp > 35 else "#22c55e"
    
    html = f"""
    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; padding: 15px;">
        <div style="background: linear-gradient(135deg, #1e40af, #3b82f6); padding: 20px; 
                    border-radius: 16px; text-align: center;">
            <div style="font-size: 14px; color: #93c5fd;">📊 Total Records</div>
            <div style="font-size: 32px; font-weight: bold; color: white;">{total['total_records']:,}</div>
        </div>
        <div style="background: linear-gradient(135deg, #b91c1c, #ef4444); padding: 20px; 
                    border-radius: 16px; text-align: center;">
            <div style="font-size: 14px; color: #fecaca;">🔥 Heatwave Alerts</div>
            <div style="font-size: 32px; font-weight: bold; color: white;">{total['heatwave_alerts']}</div>
        </div>
        <div style="background: linear-gradient(135deg, #1e3a8a, #3b82f6); padding: 20px; 
                    border-radius: 16px; text-align: center;">
            <div style="font-size: 14px; color: #bfdbfe;">🌊 Flood Alerts</div>
            <div style="font-size: 32px; font-weight: bold; color: white;">{total['flood_alerts']}</div>
        </div>
        <div style="background: linear-gradient(135deg, #166534, #22c55e); padding: 20px; 
                    border-radius: 16px; text-align: center;">
            <div style="font-size: 14px; color: #bbf7d0;">🌡️ Avg Temperature</div>
            <div style="font-size: 32px; font-weight: bold; color: white;">{avg_temp:.1f}°C</div>
        </div>
    </div>
    """
    return html


def create_recent_data_table():
    """Create table of recent records."""
    if not engine.recent_records:
        return pd.DataFrame()
    
    df = pd.DataFrame(engine.recent_records[-20:])  # Last 20 records
    
    # Select columns to display
    display_cols = ['StreamTime', 'District', 'Date', 'MaxTemp_2m', 'Precip', 'RH_2m']
    available_cols = [c for c in display_cols if c in df.columns]
    
    if available_cols:
        df = df[available_cols]
        # Format
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        if 'MaxTemp_2m' in df.columns:
            df['MaxTemp_2m'] = df['MaxTemp_2m'].round(1)
        if 'Precip' in df.columns:
            df['Precip'] = df['Precip'].round(2)
        if 'RH_2m' in df.columns:
            df['RH_2m'] = df['RH_2m'].round(1)
        
        # Rename for display
        df = df.rename(columns={
            'StreamTime': '⏱️ Time',
            'District': '📍 District',
            'Date': '📅 Date',
            'MaxTemp_2m': '🌡️ Temp (°C)',
            'Precip': '🌧️ Precip (mm)',
            'RH_2m': '💧 Humidity (%)'
        })
    
    return df.iloc[::-1]  # Reverse to show newest first


def stream_step():
    """Execute one streaming step."""
    batch = engine.get_next_batch(batch_size=10)
    
    return (
        create_station_status_html(),
        create_stats_html(),
        create_recent_data_table()
    )


def start_streaming():
    """Start the streaming simulation."""
    engine.reset()
    engine.is_streaming = True
    return (
        create_station_status_html(),
        create_stats_html(),
        create_recent_data_table(),
        gr.update(interactive=False),  # Disable start
        gr.update(interactive=True)    # Enable stop
    )


def stop_streaming():
    """Stop the streaming simulation."""
    engine.is_streaming = False
    return (
        gr.update(interactive=True),   # Enable start
        gr.update(interactive=False)   # Disable stop
    )


def auto_stream(progress=gr.Progress()):
    """Auto-stream with progress updates."""
    engine.reset()
    
    total_batches = sum(len(df) for df in engine.stations.values()) // 10
    
    for i in progress.tqdm(range(min(total_batches, 50)), desc="Streaming data..."):  # Limit to 50 batches
        batch = engine.get_next_batch(batch_size=10)
        if not batch:
            break
        time.sleep(0.3)  # Delay for visual effect
        
        yield (
            create_station_status_html(),
            create_stats_html(),
            create_recent_data_table()
        )
    
    yield (
        create_station_status_html(),
        create_stats_html(),
        create_recent_data_table()
    )


# Custom CSS
CUSTOM_CSS = """
/* Dark theme enhancements */
.gradio-container {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%) !important;
}

/* Main title */
.main-title {
    text-align: center;
    font-size: 2.5em;
    font-weight: 700;
    background: linear-gradient(135deg, #3b82f6, #22c55e);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    padding: 20px;
    margin-bottom: 20px;
}

/* Section headers */
.section-header {
    color: #94a3b8;
    font-size: 1.2em;
    font-weight: 600;
    border-bottom: 2px solid #3b82f6;
    padding-bottom: 10px;
    margin: 20px 0 15px 0;
}

/* Stream log */
.stream-log {
    font-family: 'Fira Code', 'Consolas', monospace;
    background: #0f172a;
    border: 1px solid #334155;
    border-radius: 8px;
    padding: 15px;
    max-height: 300px;
    overflow-y: auto;
}

/* Buttons */
.start-btn {
    background: linear-gradient(135deg, #22c55e, #16a34a) !important;
}

.stop-btn {
    background: linear-gradient(135deg, #ef4444, #dc2626) !important;
}
"""


def create_dashboard():
    """Create the streaming dashboard interface."""
    
    with gr.Blocks(
        title="🌊 Real-Time Weather Streaming Dashboard",
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
            🌡️ Real-Time Weather Data Streaming Dashboard 🌊
        </div>
        <div style="text-align: center; color: #64748b; margin-bottom: 20px;">
            Simulating distributed data ingestion from 10 weather stations across Nepal
        </div>
        """)
        
        # Control buttons
        with gr.Row():
            start_btn = gr.Button("▶️ Start Auto-Stream", variant="primary", size="lg")
            step_btn = gr.Button("⏭️ Stream Next Batch", variant="secondary", size="lg")
            reset_btn = gr.Button("🔄 Reset", variant="stop", size="lg")
        
        # Station Status
        gr.HTML('<div class="section-header">📡 Weather Station Status</div>')
        station_html = gr.HTML(value=create_station_status_html())
        
        # Statistics
        gr.HTML('<div class="section-header">📊 Real-Time Statistics</div>')
        stats_html = gr.HTML(value=create_stats_html())
        
        # Recent Data
        gr.HTML('<div class="section-header">📋 Recent Streamed Data</div>')
        data_table = gr.Dataframe(
            value=create_recent_data_table(),
            interactive=False,
            wrap=True
        )
        
        # Info
        with gr.Accordion("ℹ️ About This Demo", open=False):
            gr.Markdown("""
            ## Big Data Streaming Simulation
            
            This dashboard demonstrates a **real-time data streaming pipeline** that:
            
            1. **Ingests data** from 10 distributed weather stations (simulated)
            2. **Processes batches** in micro-batches (like Spark Structured Streaming)
            3. **Monitors alerts** for extreme weather (heatwaves > 40°C, floods > 100mm)
            4. **Aggregates statistics** across all data sources
            
            ### Architecture
            ```
            Weather Stations (10)  →  Streaming Layer  →  Processing Engine  →  Dashboard
                   ↓                        ↓                    ↓                  ↓
               CSV Files            Round-Robin Fetch      Batch Aggregation    Real-Time UI
            ```
            
            ### Controls
            - **Start Auto-Stream**: Automatically stream 50 batches with visual feedback
            - **Stream Next Batch**: Manually trigger one batch of data
            - **Reset**: Reset all stations to beginning
            """)
        
        # Event handlers
        start_btn.click(
            fn=auto_stream,
            outputs=[station_html, stats_html, data_table]
        )
        
        step_btn.click(
            fn=stream_step,
            outputs=[station_html, stats_html, data_table]
        )
        
        reset_btn.click(
            fn=lambda: (engine.reset(), create_station_status_html(), create_stats_html(), create_recent_data_table())[1:],
            outputs=[station_html, stats_html, data_table]
        )
    
    return app


if __name__ == "__main__":
    print("🚀 Starting Streaming Dashboard...")
    print("   Open http://localhost:7860 in your browser")
    
    app = create_dashboard()
    app.launch(
        server_name="127.0.0.1",
        server_port=7860,
        share=False
    )
