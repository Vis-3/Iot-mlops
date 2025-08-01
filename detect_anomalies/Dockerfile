# detect_anomalies/Dockerfile

FROM python:3.10.12

WORKDIR /app

# Copy source code into the container
COPY . .

# Install OS-level dependencies (optional but recommended for some packages)
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --upgrade pip && pip install --no-cache-dir \
    mlflow==2.14.0 \
    autogluon-common==1.3.1 \
    autogluon-core==1.3.1 \
    autogluon-features==1.3.1 \
    autogluon-tabular==1.3.1 \
    autogluon-timeseries==1.3.1 \
    cloudpickle==3.1.1 \
    gluonts==0.16.2 \
    numpy==1.26.4 \
    orjson==3.11.1 \
    packaging==24.2 \
    pandas==2.2.3 \
    psutil==7.0.0 \
    pyarrow==15.0.2 \
    scikit-learn==1.6.1 \
    scipy==1.15.3 \
    torch==2.6.0 \
    kafka-python

CMD ["python", "anomaly_detector.py"]
