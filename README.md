# Heatwave & Flood Prediction Pipeline

This repo provides a complete pipeline (separate Python scripts) to: ingest data, label heatwave and flood proxy targets, engineer features, train XGBoost baseline, optionally train an LSTM sequence model, evaluate and produce reports.

# Heatwave & Flood Prediction Pipeline

A complete pipeline and demo for real-time weather monitoring and disaster risk prediction. The system demonstrates:
- Data ingestion & feature engineering
- XGBoost and optional LSTM training
- Real-time streaming (OpenWeatherMap → Kafka → ML → Cassandra)
- Gradio dashboard with Live and Historical (Cassandra) views

This repository is intended as a demonstration of a Big Data streaming + ML pipeline for monitoring districts in Nepal's Terai region.

---

## Quick Start (Windows PowerShell)

1. Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Set your OpenWeatherMap API key (required for streaming):

```powershell
setx OPENWEATHERMAP_API_KEY "your_api_key_here"
# Restart PowerShell to load environment variable into current session
```

3. Start infrastructure (Docker Compose includes Kafka, Zookeeper, Cassandra, Kafka-UI):

```powershell
docker-compose up -d
```

4. Start continuous background streaming (collects OpenWeatherMap for all districts and writes to Cassandra):

```powershell
python background_streamer.py --interval 30
```

5. (Optional) Start the producer API (manual fetch/publish endpoints):

```powershell
python kafka_producer_api.py
```

6. Start the Gradio dashboard (Live + Historical tabs):

```powershell
python weather_dashboard.py
# Open http://localhost:7860
```

7. Query Cassandra directly with the provided CLI tool:

```powershell
# Show stats
python cassandra_query.py stats

# Query observations for a district
python cassandra_query.py observations --district Bara --limit 20

# Export to CSV
python cassandra_query.py export --district Bara --output bara_weather.csv
```

---

## Files of Interest

- `background_streamer.py` — Background streamer that fetches OpenWeatherMap for all monitored districts in parallel and stores observations + predictions in Cassandra. Use this for continuous ingestion.
- `kafka_producer_api.py` — FastAPI producer that can fetch weather for a city and publish to Kafka (also used by the dashboard for single fetches).
- `weather_dashboard.py` — Gradio dashboard with two main tabs:
	- Live Streaming: real-time fetch + ML predictions
	- Historical Data (Cassandra): query observations & predictions stored in Cassandra
- `cassandra_query.py` — CLI tool for ad-hoc queries, stats, and export from Cassandra.
- `train_xgb.py`, `train_lstm.py`, `evaluate.py` — model training and evaluation scripts.

---

## Cassandra: notes & schema

The dashboard and background streamer create and use the keyspace `weather_monitoring` with tables:
- `weather_observations` — raw time-series observations (partitioned by district, clustered by fetch_time)
- `weather_predictions` — stored predictions per district
- `daily_weather_summary` — aggregate daily stats

If you ever lose the keyspace, re-run the dashboard or the background streamer to re-create schema.

---

## Running locally (summary)

1. Start Docker services:
```powershell
docker-compose up -d
```
2. Start background ingestion:
```powershell
python background_streamer.py --interval 30
```
3. Start dashboard:
```powershell
python weather_dashboard.py
```
4. Run ad-hoc queries:
```powershell
python cassandra_query.py observations --district Bara --limit 10
```

---

## Requirements

Key Python packages are listed in `requirements.txt`. Important ones used by the added components:
- `gradio` — Web dashboard UI
- `cassandra-driver` — Cassandra client
- `kafka-python` — Kafka client (producer/consumer)
- `requests` — HTTP requests to OpenWeatherMap and producer API
- `xgboost`, `joblib` — ML models
- `pandas`, `numpy` — Data handling

Install all with:

```powershell
pip install -r requirements.txt
```

---

## Notes & Troubleshooting

- If the dashboard reports Cassandra not connected, ensure Docker Compose is running and the Cassandra container is healthy.
- If you see `NoBrokersAvailable` from Kafka, ensure Kafka & Zookeeper are up (`docker-compose ps`) and retry the producer/consumer.
- Background streamer requires a valid `OPENWEATHERMAP_API_KEY` environment variable.

If you'd like, I can also:
- Push a small example of how to visualize historical trends (plots) in the dashboard
- Add Docker Compose health-check automation to ensure Cassandra/Kafka restart on failures

---

## License

This project is provided for demonstration and research purposes.
