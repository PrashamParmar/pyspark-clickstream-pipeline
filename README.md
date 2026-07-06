# 🛒 E-Commerce Clickstream Analytics Pipeline

A containerized, distributed Big Data pipeline that ingests and processes **120,000+ simulated e-commerce clickstream logs**. It utilizes **Apache Spark (PySpark)** to compute user session metrics, trending products, and conversion funnels, storing the final aggregated insights in **MongoDB**, and visualizes them through a multi-lingual **Streamlit** web dashboard.

## 🏗️ Architecture

1. **Synthetic Data Generation:** A Python engine simulates realistic clickstream logs (JSON) representing user sessions, cart additions, and purchases.
2. **Distributed Processing:** A Dockerized PySpark cluster (Master/Worker) reads the logs and applies DataFrame transformations to aggregate the data.
3. **NoSQL Ingestion:** Insights are pushed directly into a MongoDB database using the Mongo-Spark Connector.
4. **Interactive UI:** A production-ready Streamlit dashboard connects to MongoDB to display Plotly-rendered KPIs, Funnels, and Trending charts. Features complete i18n support (English, Hindi, Gujarati).

## 📊 Database Schema (MongoDB `analytics_db`)

The Streamlit dashboard seamlessly reads from three aggregated collections:
* `user_session_metrics`: Tracks `user_id`, `total_duration`, and `total_actions`.
* `conversion_funnel_metrics`: Tracks drop-offs via `view_count`, `cart_count`, `purchase_count`.
* `trending_products`: Ranks items via `product_id`, `category`, and `count`.

---

## 📈 Dashboard Insights
Below is a snapshot of the live Streamlit dashboard rendering the PySpark processed data:

*(Paste a GIF or screenshot of your Streamlit Dashboard here)*

---

## 🚀 How to Run Locally

### 1. Boot up the Infrastructure
Spin up the Spark Master, Spark Worker, MongoDB, and Streamlit containers:
```bash
docker-compose up -d
```

### 2. Generate the Raw Data
Generate a fresh batch of 120,000 synthetic clickstream logs into the `data/` directory:
```bash
python scripts/data_generator.py --output-dir data/raw/clickstream --seed 42 --num-users 2000 --sessions-per-user 10 --days 14 --records-per-file 25000 --max-records 120000
```

### 3. Execute the Spark Pipeline
Submit the job to the containerized Spark cluster. *(Formatted as a single line for cross-platform compatibility):*
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 --conf "spark.mongodb.write.connection.uri=mongodb://mongo:27017" --conf "spark.mongodb.write.database=analytics_db" /app/scripts/pipeline.py --input "/data/raw/clickstream/*.json" --mongo-uri mongodb://mongo:27017 --mongo-database analytics_db
```

### 4. View the Dashboard
Once the Spark job finishes, open your browser and navigate to:
**`http://localhost:8501`**
