# ./kafka-producer/produce_flights.py
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

KAFKA_BROKER = 'localhost:29092' # Exposed Kafka port for host access
KAFKA_TOPIC = 'flight_data_raw'
CSV_FILE_PATH = './data/202412.csv' # Adjust relative path if needed

print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
try:
    # Note: Adjust api_version if compatibility issues arise
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Consider adding linger_ms=10, batch_size=16384 for better performance
        # api_version=(0, 10, 1) # Might be needed for older Kafka versions or clients
    )
    print("Successfully connected to Kafka.")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    exit(1)

print(f"Reading CSV file from {os.path.abspath(CSV_FILE_PATH)}...")
try:
    df = pd.read_csv(CSV_FILE_PATH, low_memory=False)
     # Handle potential NaN values which json.dumps dislikes
    df = df.astype(object).where(pd.notnull(df), None)
    print(f"Found {len(df)} records in CSV.")
except FileNotFoundError:
    print(f"Error: CSV file not found at {os.path.abspath(CSV_FILE_PATH)}")
    exit(1)
except Exception as e:
    print(f"Error reading CSV: {e}")
    exit(1)

count = 0
total = len(df)
start_time = time.time()

for index, row in df.iterrows():
    message = row.to_dict()
    try:
        # Use Flight_Number_Reporting_Airline or another unique field as key if desired
        # key = str(message.get('Flight_Number_Reporting_Airline', index)).encode('utf-8')
        # producer.send(KAFKA_TOPIC, key=key, value=message)
        producer.send(KAFKA_TOPIC, value=message)
        count += 1
        if count % 100 == 0: # Print progress every 100 records
            print(f"Sent {count}/{total} records...")
        # time.sleep(0.01) # Optional: Slow down production to simulate stream
    except Exception as e:
        print(f"Error sending message: {e}")
        # Decide whether to stop or continue on error
        # time.sleep(1) # Pause if errors occur rapidly

try:
    producer.flush() # Ensure all messages are sent before exiting
    print(f"Finished sending {count} records to topic '{KAFKA_TOPIC}'.")
except Exception as e:
     print(f"Error flushing producer: {e}")

end_time = time.time()
print(f"Total time taken: {end_time - start_time:.2f} seconds")

producer.close()