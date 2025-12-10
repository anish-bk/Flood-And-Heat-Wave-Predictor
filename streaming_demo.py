"""
Real-Time Streaming Simulation for Big Data Pipeline
Simulates weather data streaming from 10 distributed district weather stations.

This creates a visual demonstration of:
1. Data arriving from multiple distributed sources (simulated weather stations)
2. Real-time data aggregation and processing
3. Live predictions using trained ML models
4. Dashboard-style output showing data flow

Usage:
    python streaming_demo.py                    # Run full streaming demo
    python streaming_demo.py --speed fast       # Fast demo (less delay)
    python streaming_demo.py --speed slow       # Slow demo (more visual effect)
    python streaming_demo.py --records 50       # Limit records per station
"""

import os
import sys
import time
import random
import argparse
from datetime import datetime, timedelta
from threading import Thread, Event
from queue import Queue
import warnings
warnings.filterwarnings('ignore')

# For colored terminal output
try:
    from colorama import init, Fore, Back, Style
    init()
    COLORS_AVAILABLE = True
except ImportError:
    COLORS_AVAILABLE = False
    class Fore:
        RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = RESET = ''
    class Back:
        RED = GREEN = YELLOW = BLUE = RESET = ''
    class Style:
        BRIGHT = DIM = RESET_ALL = ''

import pandas as pd
import numpy as np

# Try importing ML libraries
try:
    import joblib
    import xgboost as xgb
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

# Try importing PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg, sum as spark_sum, count, lit
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class WeatherStation:
    """Simulates a weather station that streams data."""
    
    def __init__(self, station_id, district_name, data_file):
        self.station_id = station_id
        self.district_name = district_name
        self.data_file = data_file
        self.data = None
        self.current_index = 0
        self.is_active = False
        self.records_sent = 0
        self.last_record = None
        
        # Load data
        if os.path.exists(data_file):
            self.data = pd.read_csv(data_file, parse_dates=['Date'])
            self.data = self.data.sort_values('Date').reset_index(drop=True)
    
    def get_next_record(self):
        """Get the next record to stream."""
        if self.data is None or self.current_index >= len(self.data):
            return None
        
        record = self.data.iloc[self.current_index].to_dict()
        record['_station_id'] = self.station_id
        record['_station_name'] = self.district_name
        record['_stream_time'] = datetime.now().isoformat()
        
        self.current_index += 1
        self.records_sent += 1
        self.last_record = record
        
        return record
    
    def reset(self):
        """Reset the station to beginning."""
        self.current_index = 0
        self.records_sent = 0
        self.last_record = None


class StreamingDataPipeline:
    """
    Real-time streaming data pipeline that simulates distributed data ingestion.
    """
    
    def __init__(self, data_dir="data/district_files", speed="normal"):
        self.data_dir = data_dir
        self.stations = {}
        self.data_queue = Queue()
        self.aggregated_data = []
        self.predictions = []
        self.stop_event = Event()
        self.model = None
        self.speed = speed
        
        # Timing based on speed
        self.delays = {
            'fast': {'station': 0.05, 'record': 0.01, 'batch': 0.1},
            'normal': {'station': 0.2, 'record': 0.05, 'batch': 0.5},
            'slow': {'station': 0.5, 'record': 0.2, 'batch': 1.0}
        }
        
        # Statistics
        self.stats = {
            'total_records': 0,
            'batches_processed': 0,
            'heatwave_alerts': 0,
            'flood_alerts': 0,
            'start_time': None
        }
    
    def discover_stations(self):
        """Discover and initialize weather stations from CSV files."""
        self.print_header("DISCOVERING DISTRIBUTED WEATHER STATIONS")
        
        csv_files = sorted([f for f in os.listdir(self.data_dir) if f.endswith('.csv')])
        
        print(f"\n{Fore.CYAN}📡 Scanning for weather stations...{Style.RESET_ALL}\n")
        time.sleep(0.5)
        
        for i, csv_file in enumerate(csv_files, 1):
            file_path = os.path.join(self.data_dir, csv_file)
            district_name = csv_file.replace('.csv', '')
            station_id = f"WS-{i:03d}"
            
            station = WeatherStation(station_id, district_name, file_path)
            self.stations[station_id] = station
            
            # Visual feedback
            self.print_station_discovery(station_id, district_name, station.data)
            time.sleep(self.delays[self.speed]['station'])
        
        print(f"\n{Fore.GREEN}✅ Discovered {len(self.stations)} weather stations{Style.RESET_ALL}")
        return self.stations
    
    def print_station_discovery(self, station_id, district_name, data):
        """Print station discovery with visual effect."""
        records = len(data) if data is not None else 0
        lat = data['Latitude'].iloc[0] if data is not None and 'Latitude' in data.columns else 'N/A'
        lon = data['Longitude'].iloc[0] if data is not None and 'Longitude' in data.columns else 'N/A'
        
        print(f"   {Fore.YELLOW}🔍 Connecting...{Style.RESET_ALL}", end='\r')
        time.sleep(0.1)
        print(f"   {Fore.GREEN}✓{Style.RESET_ALL} {Fore.CYAN}{station_id}{Style.RESET_ALL} │ "
              f"{Fore.WHITE}{district_name:15}{Style.RESET_ALL} │ "
              f"📍 ({lat:.2f}, {lon:.2f}) │ "
              f"📊 {records:,} records")
    
    def load_ml_model(self):
        """Load trained ML model for real-time predictions."""
        model_path = "models/xgb_heatwave_model.joblib"
        if ML_AVAILABLE and os.path.exists(model_path):
            self.model = joblib.load(model_path)
            print(f"{Fore.GREEN}✅ Loaded ML model from {model_path}{Style.RESET_ALL}")
            return True
        return False
    
    def stream_data(self, max_records_per_station=None):
        """
        Main streaming function - simulates real-time data arrival.
        """
        self.print_header("STARTING REAL-TIME DATA STREAM")
        self.stats['start_time'] = datetime.now()
        
        print(f"\n{Fore.CYAN}🌊 Initiating data streams from {len(self.stations)} stations...{Style.RESET_ALL}\n")
        time.sleep(0.5)
        
        # Print stream header
        self.print_stream_header()
        
        batch_size = 10
        current_batch = []
        batch_num = 0
        
        # Round-robin through stations
        active_stations = list(self.stations.values())
        round_num = 0
        
        while active_stations and not self.stop_event.is_set():
            round_num += 1
            stations_to_remove = []
            
            for station in active_stations:
                # Check record limit
                if max_records_per_station and station.records_sent >= max_records_per_station:
                    stations_to_remove.append(station)
                    continue
                
                # Get next record
                record = station.get_next_record()
                
                if record is None:
                    stations_to_remove.append(station)
                    continue
                
                # Add to batch
                current_batch.append(record)
                self.stats['total_records'] += 1
                
                # Print streaming visualization
                self.print_stream_record(station, record)
                
                time.sleep(self.delays[self.speed]['record'])
                
                # Process batch
                if len(current_batch) >= batch_size:
                    batch_num += 1
                    self.process_batch(current_batch, batch_num)
                    current_batch = []
                    time.sleep(self.delays[self.speed]['batch'])
            
            # Remove inactive stations
            for station in stations_to_remove:
                active_stations.remove(station)
                self.print_station_complete(station)
        
        # Process remaining records
        if current_batch:
            batch_num += 1
            self.process_batch(current_batch, batch_num)
        
        self.print_stream_summary()
    
    def print_stream_header(self):
        """Print the streaming visualization header."""
        print(f"{'─'*80}")
        print(f"  {'Station':<12} │ {'District':<15} │ {'Date':<12} │ "
              f"{'Temp':>6} │ {'Precip':>7} │ {'Status':<10}")
        print(f"{'─'*80}")
    
    def print_stream_record(self, station, record):
        """Print a single streaming record with visual effects."""
        date_str = str(record.get('Date', 'N/A'))[:10]
        temp = record.get('MaxTemp_2m', 0)
        precip = record.get('Precip', 0)
        
        # Color based on values
        temp_color = Fore.RED if temp > 40 else Fore.YELLOW if temp > 35 else Fore.GREEN
        precip_color = Fore.BLUE if precip > 50 else Fore.CYAN if precip > 10 else Fore.WHITE
        
        # Status indicator
        status = ""
        if temp > 40:
            status = f"{Fore.RED}🔥 HOT{Style.RESET_ALL}"
            self.stats['heatwave_alerts'] += 1
        elif precip > 100:
            status = f"{Fore.BLUE}🌊 FLOOD{Style.RESET_ALL}"
            self.stats['flood_alerts'] += 1
        else:
            status = f"{Fore.GREEN}✓ OK{Style.RESET_ALL}"
        
        print(f"  {Fore.CYAN}{station.station_id:<12}{Style.RESET_ALL} │ "
              f"{station.district_name:<15} │ "
              f"{date_str:<12} │ "
              f"{temp_color}{temp:>5.1f}°{Style.RESET_ALL} │ "
              f"{precip_color}{precip:>6.1f}mm{Style.RESET_ALL} │ "
              f"{status}")
    
    def process_batch(self, batch, batch_num):
        """Process a batch of streamed records."""
        self.stats['batches_processed'] += 1
        
        print(f"\n{Fore.MAGENTA}{'─'*80}{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}📦 PROCESSING BATCH #{batch_num} ({len(batch)} records){Style.RESET_ALL}")
        
        # Aggregate statistics
        df = pd.DataFrame(batch)
        
        avg_temp = df['MaxTemp_2m'].mean() if 'MaxTemp_2m' in df.columns else 0
        total_precip = df['Precip'].sum() if 'Precip' in df.columns else 0
        districts = df['_station_name'].nunique()
        
        print(f"   📊 Avg Temp: {Fore.YELLOW}{avg_temp:.1f}°C{Style.RESET_ALL} │ "
              f"Total Precip: {Fore.CYAN}{total_precip:.1f}mm{Style.RESET_ALL} │ "
              f"Districts: {districts}")
        
        # Run predictions if model available
        if self.model is not None:
            self.run_batch_predictions(df)
        
        print(f"{Fore.MAGENTA}{'─'*80}{Style.RESET_ALL}\n")
        
        # Store aggregated data
        self.aggregated_data.extend(batch)
    
    def run_batch_predictions(self, df):
        """Run ML predictions on batch data."""
        try:
            # Prepare features (simplified)
            feature_cols = ['Precip', 'MaxTemp_2m', 'RH_2m', 'TempRange_2m', 
                           'WindSpeed_10m', 'WindSpeed_50m']
            available_cols = [c for c in feature_cols if c in df.columns]
            
            if len(available_cols) < 3:
                return
            
            X = df[available_cols].fillna(0)
            
            # Make predictions
            if isinstance(self.model, xgb.Booster):
                dmatrix = xgb.DMatrix(X)
                preds = self.model.predict(dmatrix)
            else:
                preds = self.model.predict_proba(X)[:, 1] if hasattr(self.model, 'predict_proba') else self.model.predict(X)
            
            # Count high-risk predictions
            high_risk = sum(1 for p in preds if p > 0.5)
            
            if high_risk > 0:
                print(f"   {Fore.RED}⚠️  ML ALERT: {high_risk} high-risk predictions in batch!{Style.RESET_ALL}")
            else:
                print(f"   {Fore.GREEN}✓  ML: All predictions within normal range{Style.RESET_ALL}")
                
        except Exception as e:
            print(f"   {Fore.YELLOW}⚠️  Prediction skipped: {str(e)[:50]}{Style.RESET_ALL}")
    
    def print_station_complete(self, station):
        """Print when a station completes streaming."""
        print(f"\n   {Fore.YELLOW}📡 Station {station.station_id} ({station.district_name}) "
              f"completed - {station.records_sent} records sent{Style.RESET_ALL}\n")
    
    def print_stream_summary(self):
        """Print final streaming summary."""
        duration = (datetime.now() - self.stats['start_time']).total_seconds()
        
        self.print_header("STREAMING COMPLETE - SUMMARY")
        
        print(f"""
{Fore.CYAN}📊 STREAMING STATISTICS{Style.RESET_ALL}
{'─'*50}
   Total Records Streamed:  {Fore.GREEN}{self.stats['total_records']:,}{Style.RESET_ALL}
   Batches Processed:       {Fore.GREEN}{self.stats['batches_processed']}{Style.RESET_ALL}
   Heatwave Alerts:         {Fore.RED}{self.stats['heatwave_alerts']}{Style.RESET_ALL}
   Flood Alerts:            {Fore.BLUE}{self.stats['flood_alerts']}{Style.RESET_ALL}
   Duration:                {Fore.YELLOW}{duration:.1f} seconds{Style.RESET_ALL}
   Throughput:              {Fore.GREEN}{self.stats['total_records']/duration:.1f} records/sec{Style.RESET_ALL}
{'─'*50}

{Fore.CYAN}📡 STATION SUMMARY{Style.RESET_ALL}
{'─'*50}""")
        
        for station_id, station in self.stations.items():
            print(f"   {station_id} │ {station.district_name:15} │ "
                  f"{Fore.GREEN}{station.records_sent:,} records{Style.RESET_ALL}")
        
        print(f"{'─'*50}")
    
    def print_header(self, title):
        """Print a formatted header."""
        print(f"\n{'='*80}")
        print(f"{Fore.CYAN}{Style.BRIGHT}  {title}{Style.RESET_ALL}")
        print(f"{'='*80}")


class SparkStreamingDemo:
    """
    Spark Structured Streaming simulation for more realistic big data demo.
    """
    
    def __init__(self, data_dir="data/district_files"):
        self.data_dir = data_dir
        self.spark = None
    
    def initialize_spark(self):
        """Initialize Spark for streaming."""
        if not PYSPARK_AVAILABLE:
            print(f"{Fore.RED}❌ PySpark not available{Style.RESET_ALL}")
            return False
        
        self.spark = SparkSession.builder \
            .appName("WeatherStreamingDemo") \
            .config("spark.sql.shuffle.partitions", "4") \
            .master("local[*]") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        return True
    
    def simulate_streaming(self, batch_interval=2):
        """Simulate streaming using micro-batches."""
        print(f"\n{Fore.CYAN}🚀 Starting Spark Streaming Simulation...{Style.RESET_ALL}\n")
        
        # Read all district files
        csv_files = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.csv')]
        
        # Read as Spark DataFrame
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_files)
        
        total_rows = df.count()
        batch_size = 1000
        num_batches = (total_rows // batch_size) + 1
        
        print(f"   Total Records: {total_rows:,}")
        print(f"   Batch Size: {batch_size}")
        print(f"   Total Batches: {num_batches}\n")
        
        # Simulate micro-batches
        for batch_num in range(1, min(num_batches + 1, 11)):  # Limit to 10 batches for demo
            print(f"{Fore.YELLOW}📦 Processing Micro-Batch {batch_num}/{num_batches}...{Style.RESET_ALL}")
            
            # Get batch data
            batch_df = df.limit(batch_size * batch_num).subtract(df.limit(batch_size * (batch_num - 1)))
            
            # Aggregate
            summary = batch_df.groupBy("District").agg(
                count("*").alias("records"),
                avg("MaxTemp_2m").alias("avg_temp"),
                spark_sum("Precip").alias("total_precip")
            )
            
            print(f"   {Fore.GREEN}✓ Batch processed{Style.RESET_ALL}")
            summary.show(5, truncate=False)
            
            time.sleep(batch_interval)
        
        print(f"\n{Fore.GREEN}✅ Streaming simulation complete{Style.RESET_ALL}")
    
    def stop(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Real-Time Streaming Demo for Big Data Pipeline")
    parser.add_argument('--speed', choices=['fast', 'normal', 'slow'], default='normal',
                       help='Streaming speed (fast/normal/slow)')
    parser.add_argument('--records', type=int, default=20,
                       help='Max records per station (default: 20)')
    parser.add_argument('--mode', choices=['simple', 'spark'], default='simple',
                       help='Demo mode: simple (visual) or spark (PySpark)')
    args = parser.parse_args()
    
    print(f"""
{Fore.CYAN}{Style.BRIGHT}
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   🌡️  REAL-TIME WEATHER DATA STREAMING DEMO  🌊                              ║
║                                                                              ║
║   Simulating distributed data ingestion from 10 weather stations            ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}
""")
    
    print(f"   Mode: {Fore.YELLOW}{args.mode.upper()}{Style.RESET_ALL}")
    print(f"   Speed: {Fore.YELLOW}{args.speed.upper()}{Style.RESET_ALL}")
    print(f"   Records per Station: {Fore.YELLOW}{args.records}{Style.RESET_ALL}")
    
    if args.mode == 'spark':
        # Spark streaming demo
        demo = SparkStreamingDemo()
        if demo.initialize_spark():
            try:
                demo.simulate_streaming()
            finally:
                demo.stop()
    else:
        # Simple visual streaming demo
        pipeline = StreamingDataPipeline(speed=args.speed)
        
        # Discover stations
        pipeline.discover_stations()
        
        # Load ML model
        pipeline.load_ml_model()
        
        # Start streaming
        try:
            pipeline.stream_data(max_records_per_station=args.records)
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠️ Streaming interrupted by user{Style.RESET_ALL}")
            pipeline.print_stream_summary()
    
    print(f"\n{Fore.GREEN}✅ Demo Complete!{Style.RESET_ALL}\n")


if __name__ == "__main__":
    main()
