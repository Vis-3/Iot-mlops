FROM python:3.10.12

# Install system dependencies for AutoGluon and pandas parquet support
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    python3-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy training script and any local data if needed
COPY train_model.py .
# Copy data folder if you want data baked in, or mount as volume instead
# COPY ../data ./data

ENV MLFLOW_TRACKING_URI=http://mlflow:5000

CMD ["python", "train_model.py"]
