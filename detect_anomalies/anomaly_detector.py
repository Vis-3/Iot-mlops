import os
import logging
import pandas as pd
import mlflow
from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime
from prometheus_client import start_http_server, Counter, Gauge

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
MLFLOW_TRACKING_URI = 'http://mlflow:5000'
MODEL_NAME = "AutoGluonIoTAnomalyDetector"
MODEL_ALIAS = "production"
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
INPUT_KAFKA_TOPIC = 'iot_sensor_data'
OUTPUT_KAFKA_TOPIC = 'iot_anomalies'
ANOMALY_THRESHOLD_PERCENT = 0.10

# --- Prometheus Metrics ---
anomaly_count = Counter(
    'anomaly_total',
    'Total number of anomalies detected',
    ['device_type']
)

prediction_value = Gauge(
    'predicted_value',
    'Predicted value from model',
    ['device_id']
)

# --- Anomaly Detector Logic ---
def run_anomaly_detector():
    logging.info("Starting IoT Anomaly Detector service...")

    # Start Prometheus metrics server on port 8000
    start_http_server(8000)

    # Set MLflow tracking URI
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # Use the registered model and alias
    model_uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
    logging.info(f"Loading model from registered URI: {model_uri}")

    try:
        predictor_wrapper = mlflow.pyfunc.load_model(model_uri)
        logging.info(f"Model loaded successfully from: {model_uri}")
    except Exception as e:
        logging.error(f"Failed to load MLflow model from '{model_uri}': {e}", exc_info=True)
        return

    # Kafka Consumer Setup
    try:
        consumer = KafkaConsumer(
            INPUT_KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
            group_id='anomaly_detector_group'
        )
        logging.info(f"Kafka consumer initialized for topic: {INPUT_KAFKA_TOPIC}")
    except Exception as e:
        logging.error(f"Error initializing Kafka consumer: {e}", exc_info=True)
        return

    # Kafka Producer Setup
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info(f"Kafka producer initialized for topic: {OUTPUT_KAFKA_TOPIC}")
    except Exception as e:
        logging.error(f"Error initializing Kafka producer: {e}", exc_info=True)
        return

    logging.info("Listening for new IoT data...")

    for message in consumer:
        try:
            iot_data = message.value
            logging.info(f"Received data for anomaly check: {iot_data}")

            device_id = iot_data.get('device_id')
            device_type = iot_data.get('device_type', 'unknown')
            timestamp_str = iot_data.get('timestamp')
            actual_value = iot_data.get('metric_value')

            if not all([device_id, timestamp_str, actual_value is not None]):
                logging.warning(f"Skipping malformed message: {iot_data}")
                continue

            prediction_input_df = pd.DataFrame({
                'item_id': [device_id],
                'timestamp': [pd.to_datetime(timestamp_str)]
            })

            predictions_df = predictor_wrapper.predict(prediction_input_df)

            if not predictions_df.empty:
                predicted_value = predictions_df.iloc[0]['mean']
            else:
                logging.warning(f"Model returned empty prediction for {device_id} at {timestamp_str}. Skipping.")
                continue

            logging.info(f"Device: {device_id}, Timestamp: {timestamp_str}, Actual: {actual_value:.4f}, Predicted: {predicted_value:.4f}")

            # --- Prometheus Metrics ---
            prediction_value.labels(device_id=device_id).set(predicted_value)

            is_anomaly = False
            if actual_value is not None and predicted_value is not None:
                if abs(predicted_value) < 1e-6:
                    if abs(actual_value) > ANOMALY_THRESHOLD_PERCENT:
                        is_anomaly = True
                else:
                    percentage_deviation = abs((actual_value - predicted_value) / predicted_value)
                    if percentage_deviation > ANOMALY_THRESHOLD_PERCENT:
                        is_anomaly = True

            if is_anomaly:
                anomaly_count.labels(device_type=device_type).inc()

            anomaly_message = {
                'device_id': device_id,
                'device_type': device_type,
                'timestamp': timestamp_str,
                'actual_value': actual_value,
                'predicted_value': predicted_value,
                'is_anomaly': is_anomaly,
                'anomaly_threshold_percent': ANOMALY_THRESHOLD_PERCENT,
                'model_name': MODEL_NAME,
                'model_alias': MODEL_ALIAS,
                'detection_time': datetime.now().isoformat()
            }

            producer.send(OUTPUT_KAFKA_TOPIC, anomaly_message)
            logging.info(f"Anomaly result for {device_id}: {is_anomaly}. Sent to {OUTPUT_KAFKA_TOPIC}")

        except Exception as e:
            logging.error(f"Error processing Kafka message: {e}", exc_info=True)

if __name__ == "__main__":
    run_anomaly_detector()
