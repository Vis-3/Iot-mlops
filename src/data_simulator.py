# src/iot_data_simulator.py

import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import uuid # For unique device IDs

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093' # Connect to Kafka via the exposed host port
KAFKA_TOPIC = 'iot_sensor_data'
NUM_DEVICES = 5
SIMULATION_DURATION_SECONDS = 3600 # Simulate 1 hour of data
INTERVAL_SECONDS = 1 # Send data every 1 second per device

# Define different device types and their normal operating ranges
DEVICE_CONFIGS = {
    'temperature_sensor': {'metric_name': 'temperature', 'base_value': 25.0, 'variance': 1.5, 'anomaly_prob': 0.05, 'anomaly_magnitude': 10.0},
    'pressure_sensor': {'metric_name': 'pressure', 'base_value': 100.0, 'variance': 5.0, 'anomaly_prob': 0.03, 'anomaly_magnitude': 20.0},
    'humidity_sensor': {'metric_name': 'humidity', 'base_value': 60.0, 'variance': 3.0, 'anomaly_prob': 0.02, 'anomaly_magnitude': 15.0},
}

# --- Kafka Producer Setup ---
# Kafka producer configuration
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'iot-simulator-producer'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    """Callback for Kafka message delivery report."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    # else:
    #     print(f"Message delivered to topic '{msg.topic()}' partition [{msg.partition()}] at offset {msg.offset()}")

def generate_device_id(device_type):
    """Generates a unique ID for a device."""
    return f"{device_type}_{str(uuid.uuid4())[:8]}"

def generate_iot_data(device_id, device_type, timestamp, current_step, total_steps):
    """Generates a single IoT sensor data point with potential anomalies."""
    config = DEVICE_CONFIGS[device_type]
    metric_name = config['metric_name']
    base_value = config['base_value']
    variance = config['variance']
    anomaly_prob = config['anomaly_prob']
    anomaly_magnitude = config['anomaly_magnitude']

    # Simulate a gentle trend over time (e.g., daily cycle)
    # This makes the "normal" data slightly dynamic
    trend_factor = 0.5 * (1 + (current_step / total_steps) * 2 - 1) * base_value * 0.1 # Small 10% variation
    
    # Base value with some natural variance
    value = base_value + random.gauss(0, variance) + trend_factor

    # Introduce anomalies based on probability
    if random.random() < anomaly_prob:
        anomaly_type = random.choice(['spike', 'drop', 'shift'])
        if anomaly_type == 'spike':
            value += anomaly_magnitude * random.uniform(0.8, 1.2)
            is_anomaly = True
        elif anomaly_type == 'drop':
            value -= anomaly_magnitude * random.uniform(0.8, 1.2)
            is_anomaly = True
        elif anomaly_type == 'shift': # A sustained shift for a few points
            value += anomaly_magnitude * random.uniform(-0.5, 0.5)
            is_anomaly = True
        else:
            is_anomaly = False # Should not happen, but for safety
    else:
        is_anomaly = False

    data_point = {
        "timestamp": timestamp.isoformat(),
        "device_id": device_id,
        "device_type": device_type,
        "metric_name": metric_name,
        "metric_value": round(value, 3), # Round to 3 decimal places
        "is_anomaly": is_anomaly # Label for later use in evaluation
    }
    return data_point

def simulate_and_publish():
    print(f"Starting IoT data simulation to Kafka topic: {KAFKA_TOPIC}")
    print(f"Targeting Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")

    devices = []
    device_types_list = list(DEVICE_CONFIGS.keys())
    for _ in range(NUM_DEVICES):
        device_type = random.choice(device_types_list)
        devices.append({'id': generate_device_id(device_type), 'type': device_type})

    total_steps = int(SIMULATION_DURATION_SECONDS / INTERVAL_SECONDS)
    start_time = time.time()
    current_step = 0

    try:
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time > SIMULATION_DURATION_SECONDS:
                print("Simulation duration reached. Stopping.")
                break

            for device in devices:
                timestamp = datetime.now()
                data_point = generate_iot_data(device['id'], device['type'], timestamp, current_step, total_steps)
                
                # Convert dict to JSON string
                message_value = json.dumps(data_point)
                
                # Use device_id as key for Kafka partitioning (optional, but good practice)
                producer.produce(
                    KAFKA_TOPIC, 
                    key=device['id'].encode('utf-8'), 
                    value=message_value.encode('utf-8'), 
                    callback=delivery_report
                )
                
                # Poll to deliver messages (important for non-blocking send)
                producer.poll(0) # Poll immediately, don't block

            time.sleep(INTERVAL_SECONDS)
            current_step += 1
            if current_step % 10 == 0: # Print status every 10 steps
                print(f"Simulated {current_step} data points. Elapsed: {int(elapsed_time)}s")

    except KeyboardInterrupt:
        print("\nSimulation interrupted by user.")
    finally:
        # Wait for any outstanding messages to be delivered and stop the producer
        print("Flushing outstanding messages...")
        producer.flush(timeout=10) # Wait up to 10 seconds for messages to be delivered
        print("Simulation finished.")

if __name__ == "__main__":
    simulate_and_publish()