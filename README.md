Real-Time IoT Anomaly Detection MLOps Pipeline

Overview

This project implements a scalable IoT Anomaly Detection System using machine learning and modern observability tools. It ingests real-time sensor data via Kafka, runs anomaly detection using an AutoGluon model managed by MLflow, and exposes anomaly metrics for monitoring with Prometheus and Grafana dashboards.

Features

Model Training & Experiment Tracking: Trains a time series model using AutoGluon and logs the experiment with MLflow.
Model Registry & Versioning: Registers the trained model in the MLflow Model Registry for version control.
Real-time Data Streaming: Simulates IoT data streaming using a Python producer and Kafka topics.
Anomlay Detection Service: A Python service that loads the model from MLflow, consumes streaming data from Kafka, and flags anomalies in real-time.
Monitoring & Visualization: Observes the pipeline's health and detected anomalies using Prometheus and Grafana.
Containerized Environment: All services are orchestrated via Docker Compose for easy setup and deployment.

Data Producer: A Python script simulates an IoT device, pushing sensor readings to a Kafka topic.
Kafka: Serves as the central nervous system for streaming data.
MLflow: Acts as the Model Registry, storing the trained model, its metadata, and its artifacts.
Anomaly Detector: The core of the project. It's a consumer that pulls data from Kafka, uses the MLflow-registered model to predict the next value, and compares it to the actual data point to detect anomalies.
Prometheus & Grafana: Scrape metrics from the running services and provide a beautiful, real-time dashboard for monitoring anomalies and system health.

This is a fantastic final step for a project. A great README not only documents the code but also sells the project and guides users.

Here is a comprehensive and "amazing" README you can use, tailored to your MLOps pipeline. Just copy and paste it into a README.md file in your repository's root directory.

🤖 Real-Time IoT Anomaly Detection MLOps Pipeline
An end-to-end MLOps pipeline for real-time anomaly detection on streaming IoT sensor data using AutoGluon, MLflow, Kafka, Prometheus, and Grafana.

✨ Features
Model Training & Experiment Tracking: Trains a time series model using AutoGluon and logs the experiment with MLflow.

Model Registry & Versioning: Registers the trained model in the MLflow Model Registry for version control.

Real-time Data Streaming: Simulates IoT data streaming using a Python producer and Kafka topics.

Anomlay Detection Service: A Python service that loads the model from MLflow, consumes streaming data from Kafka, and flags anomalies in real-time.

Monitoring & Visualization: Observes the pipeline's health and detected anomalies using Prometheus and Grafana.

Containerized Environment: All services are orchestrated via Docker Compose for easy setup and deployment.

🏗️ Architecture
The pipeline is built on a microservices architecture, with each component running in its own Docker container.

Code snippet

graph TD
    A[Data Producer] --> B(Kafka Topic: iot_data)
    B --> C{Anomaly Detector Service}
    C --> D[MLflow Server & Registry]
    C --> E(Kafka Topic: iot_anomalies)
    E --> F[Prometheus Scraper]
    subgraph Monitoring Stack
        F --> G[Prometheus]
        G --> H[Grafana Dashboard]
    end
    D --> I[Anomaly Detector Service: Model Loading]
    C --> I
Data Producer: A Python script simulates an IoT device, pushing sensor readings to a Kafka topic.

Kafka: Serves as the central nervous system for streaming data.

MLflow: Acts as the Model Registry, storing the trained model, its metadata, and its artifacts.

Anomaly Detector: The core of the project. It's a consumer that pulls data from Kafka, uses the MLflow-registered model to predict the next value, and compares it to the actual data point to detect anomalies.

Prometheus & Grafana: Scrape metrics from the running services and provide a beautiful, real-time dashboard for monitoring anomalies and system health.

Getting Started
Prerequisites
Docker installed and running.
Docker Compose (comes with Docker Desktop).
Python 3.8+ and pip for running the training and data producer scripts.

Installation & Setup

Clone the repository:

Bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
Start all services:
This command will build the Docker images and start all the containers.

Bash
docker compose up -d --build
This may take a few minutes for the initial setup.

Usage
1. Train and Log the Model
First, you need to train the model and register it with MLflow.
Ensure you are in the iot-mlops root directory.
Run the training script from the train folder:

Bash
cd train
python train_model.py

Or you could simulate your own data first using data_simulator.py and then comsuming_historical_data.py which would generate the paraquet file that can be used by the train_model.py

The script will train an AutoGluon time series model, log the run, and register the model as AutoGluonIoTAnomalyDetector in the MLflow Model Registry.

2. View and Promote the Model
Now, go to the MLflow UI to confirm the model is registered and to set its stage.

Open your browser and navigate to: http://localhost:5000

Click on Models in the left sidebar. You should see AutoGluonIoTAnomalyDetector. CLick on the version of the model you plan on using, change the alias to 'production'.

Run the Anomaly Detector & Data Producer
With the model promoted, you can start the data stream and the detector.
Run the simulator script again and monitor the logs using - docker compose logs -f anomaly-detector

onitor with Grafana
Open your browser and navigate to the Grafana dashboard: http://localhost:3000

Login with the default credentials: admin / admin
Add Prometheus as a data source by pointing it to http://prometheus:9090 (using the Docker service name).
Create dashboards to visualize your iot_data and iot_anomalies metrics.(have a basic grafana.json file for the dashboard that can be imported, the case is sensitive so make sure it matches the json file)

Contributing
Contributions are welcome! If you have suggestions or find a bug, please open an issue or submit a pull request.
