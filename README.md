# Clickstream Analytics (PySpark → MongoDB → Streamlit)

A containerized Big Data pipeline that processes **120,000+ e-commerce clickstream logs** using **PySpark**, stores aggregated results in **local MongoDB**, and visualizes them with a production-ready **Streamlit dashboard**.

---

## Architecture

1. **Data generation (optional)**: `scripts/data_generator.py`
2. **Spark pipeline**: `scripts/pipeline.py`
   - Computes:
     - `user_session_metrics`
     - `trending_products`
     - `conversion_funnel_metrics`
3. **MongoDB**: `analytics_db`
4. **Streamlit Dashboard**: `app.py`
   - KPIs + Funnel (Plotly) + Trending products (Plotly) + Data table
   - i18n support: English / Hindi / Gujarati
   - Robust empty/missing-field handling

---

## Requirements

- Docker + Docker Compose
- Python (for local development; dashboard also runs in Docker)
- Ports:
  - MongoDB: `27017`
  - Streamlit: `8501`

---

## MongoDB Schema (Collections)

Dashboard reads from these collections inside database **`analytics_db`**:

1. `user_session_metrics`
   - Expected fields (pipeline writes aggregates):
     - `user_id`
     - `total_duration` (may be present)
     - `total_actions` (used by dashboard)

2. `conversion_funnel_metrics`
   - Expected fields:
     - `view_count`, `cart_count`, `purchase_count`

3. `trending_products`
   - Expected fields:
     - `product_id`, `category`, `count`

> The dashboard will not crash if collections are empty or if column names slightly differ.

---

## Quick Start (Local / Docker)

### 1) Start MongoDB
```bat
cd d:\RESUME_PROJECT\clickstream_analytics
docker-compose up -d mongo
```

### 2) Start Spark (optional)
Start Spark master + worker:
```bat
docker-compose up -d spark-master spark-worker
```

### 3) Run the PySpark pipeline
How you run the pipeline depends on how you execute code inside containers in your setup.

Typical approach:
- Generate raw clickstream JSON (optional):
  - `docker exec ... python scripts/data_generator.py ...`
- Run pipeline to write to MongoDB:
  - `docker exec ... python scripts/pipeline.py ...`

> If your pipeline is already runnable in your existing workflow, run it using the same steps you used previously.

### 4) Start Streamlit dashboard (Docker)
This runs Streamlit and connects it to MongoDB.

```bat
docker-compose -f docker-compose.streamlit.yml up -d --build
```

Open the dashboard:
- Local: `http://localhost:8501`
- From another device on the LAN:
  - `http://<YOUR_MACHINE_IP>:8501`

---

## Global / Worldwide Sharing with ngrok (Recommended)

### 1) Install ngrok
- Install from https://ngrok.com
- Authenticate:
```bat
ngrok config add-authtoken <YOUR_NGROK_TOKEN>
```

### 2) Expose Streamlit port
```bat
ngrok http 8501
```

ngrok will print a public HTTPS URL like:
- `https://xxxx-xx-xx-xx.ngrok-free.app`

Share that link worldwide.

> Ensure your Streamlit container is already running on `localhost:8501`.

---

## Run Dashboard Locally (Non-Docker)

If you prefer to run Streamlit directly on your PC:

```bat
cd d:\RESUME_PROJECT\clickstream_analytics
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install streamlit plotly pymongo pandas
streamlit run app.py
```

---

## Notes / Troubleshooting

### Dashboard shows warning about empty MongoDB
- Run the PySpark pipeline first so the collections are populated.

### Mongo connection issues
- The dashboard expects MongoDB at:
  - Docker mode: `mongodb://mongo:27017/`
  - Local mode: `mongodb://localhost:27017/`

---

## Files

- `scripts/pipeline.py` : Spark computations and writes to MongoDB
- `scripts/data_generator.py` : Generates synthetic clickstream JSON
- `app.py` : Streamlit dashboard with i18n + Plotly charts
- `docker-compose.yml` : Spark + Mongo services
- `docker-compose.streamlit.yml` : Streamlit + Mongo connectivity
- `streamlit/Dockerfile` : Container build for Streamlit

