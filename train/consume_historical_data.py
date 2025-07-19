# train/consume_historical_data.py

import json
import pandas as pd
from confluent_kafka import Consumer, KafkaException
from datetime import datetime, timedelta
import sys
import os

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
KAFKA_TOPIC = 'iot_sensor_data'
CONSUMER_GROUP_ID = 'historical_data_consumer_group'
OUTPUT_FILE = '../data/historical_iot_data.parquet' # Relative path to a 'data' folder
CONSUMPTION_DURATION_SECONDS = 300 # Consume data for 5 minutes (300 seconds)

# --- Kafka Consumer Setup ---
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': CONSUMER_GROUP_ID,
    'auto.offset.reset': 'earliest' # Start reading from the beginning of the topic
}

consumer = Consumer(consumer_config)

def consume_data_to_dataframe():
    try:
        consumer.subscribe([KAFKA_TOPIC])
        print(f"Subscribed to Kafka topic: {KAFKA_TOPIC}")
        print(f"Consuming data for {CONSUMPTION_DURATION_SECONDS} seconds...")

        records = []
        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < CONSUMPTION_DURATION_SECONDS:
            msg = consumer.poll(timeout=1.0) # Poll for message with a timeout

            if msg is None:
                # print("Waiting for messages...")
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event - not an error
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                      (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Message successfully received
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    records.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e} - Message: {msg.value().decode('utf-8')}")
                except Exception as e:
                    print(f"An unexpected error occurred: {e} - Message: {msg.value().decode('utf-8')}")
        
        print(f"Finished consuming data. Total records collected: {len(records)}")
        if not records:
            print("No records collected. Ensure data simulator is running and Kafka is accessible.")
            return pd.DataFrame() # Return empty DataFrame if no records

        df = pd.DataFrame(records)
        return df

    except KeyboardInterrupt:
        print("\nConsumption interrupted by user.")
        return pd.DataFrame(records) # Return collected data on interrupt
    finally:
        consumer.close()
        print("Kafka consumer closed.")

if __name__ == "__main__":
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    df_historical = consume_data_to_dataframe()

    if not df_historical.empty:
        # Basic preprocessing for training
        df_historical['timestamp'] = pd.to_datetime(df_historical['timestamp'])
        
        # Sort by device_id and timestamp, crucial for time series
        df_historical = df_historical.sort_values(by=['device_id', 'timestamp']).reset_index(drop=True)

        df_historical.to_parquet(OUTPUT_FILE, index=False)
        print(f"Historical data saved to: {OUTPUT_FILE}")
    else:
        print("No data to save.")