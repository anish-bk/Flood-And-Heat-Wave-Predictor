# Heatwave & Flood Prediction Pipeline

This repo provides a complete pipeline (separate Python scripts) to: ingest data, label heatwave and flood proxy targets, engineer features, train XGBoost baseline, optionally train an LSTM sequence model, evaluate and produce reports.

## How to Run

### 1. Create virtualenv and install requirements

**Linux/macOS:**
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Prepare input data

Place your input CSV at `data/weather.csv` with the required weather columns.

### 3. Run pipeline steps

```bash
# Step 1: Data Ingestion - converts raw CSV to parquet format
python data_ingest.py --input data/weather.csv --out_dir data/processed

# Step 2: Labeling - creates heatwave and flood proxy labels
python labeling.py --in_dir data/processed --out_dir data/processed

# Step 3: Feature Engineering - generates features for model training
python features.py --in_dir data/processed --out_dir data/processed

# Step 4: Train XGBoost Model
python train_xgb.py --features data/processed/features.parquet --out_dir models/

# Step 5: Evaluate Model - generates metrics and plots
python evaluate.py --features data/processed/features.parquet --model models/xgb_heatwave_model.joblib --out_dir results/
```

### 4. Optional: Train LSTM Model (PyTorch)

Requires sufficient sequential data:
```bash
python train_lstm.py --features data/processed/features.parquet --out_dir models/
```

You can also specify the target and timesteps:
```bash
python train_lstm.py --features data/processed/features.parquet --out_dir models/ --target heatwave --timesteps 14
python train_lstm.py --features data/processed/features.parquet --out_dir models/ --target flood_proxy --timesteps 14
```

## Project Structure

```
├── data_ingest.py      # Data ingestion script
├── labeling.py         # Heatwave and flood labeling
├── features.py         # Feature engineering
├── train_xgb.py        # XGBoost model training
├── train_lstm.py       # LSTM model training (PyTorch)
├── evaluate.py         # Model evaluation and plotting
├── utils.py            # Utility functions
├── requirements.txt    # Python dependencies
└── README.md           # This file
```

## Requirements

- Python 3.8+
- pandas, numpy, scikit-learn
- xgboost
- torch (PyTorch)
- matplotlib, seaborn
- shap