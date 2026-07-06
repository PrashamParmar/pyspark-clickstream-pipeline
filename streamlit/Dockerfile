FROM python:3.12-slim

# Install system deps (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first (better layer caching)
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt \
    streamlit plotly pymongo pandas

# Copy app code
COPY app.py /app/app.py

# Expose default Streamlit port
EXPOSE 8501

# Streamlit expects host=0.0.0.0 for container access
CMD ["python", "-m", "streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]

