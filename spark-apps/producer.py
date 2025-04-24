import csv
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = 'localhost:29092' # External port exposed in docker-compose
KAFKA_TOPIC = 'flight_data'
CSV_FILE = '/opt/bitnami/spark/data/sample.csv' # Path inside the Spark containers

def create_producer():
    print(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all', # Ensure message is received by all replicas
                retries=5   # Retry sending messages on failure
            )
            print("Successfully connected to Kafka.")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka broker at {KAFKA_BROKER} not available. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"An error occurred while connecting to Kafka: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)


def send_data(producer, topic, file_path):
    print(f"Reading data from {file_path} and sending to Kafka topic '{topic}'...")
    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            count = 0
            for row in reader:
                # Basic type conversion (optional, but good practice)
                for key, value in row.items():
                    if value: # Avoid converting empty strings
                        try:
                            if '.' in value:
                                row[key] = float(value)
                            elif value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                                 row[key] = int(value)
                        except ValueError:
                            pass # Keep as string if conversion fails
                    elif key in ['DepDelay', 'DepDelayMinutes', 'ArrDelay', 'ArrDelayMinutes', 'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']:
                        # Handle potentially empty numeric fields - default to 0 or None
                        row[key] = 0.0 # Or None if you prefer

                try:
                    future = producer.send(topic, value=row)
                    # Optional: Wait for ack to ensure message is sent
                    # result = future.get(timeout=60)
                    # print(f"Sent record: {count+1}, Offset: {result.offset}")
                    count += 1
                    if count % 100 == 0: # Print progress
                         print(f"Sent {count} records...")
                    # time.sleep(0.01) # Optional: Slow down sending rate
                except Exception as e:
                    print(f"Error sending record {count+1}: {row}")
                    print(f"Error details: {e}")

            producer.flush() # Ensure all messages are sent before exiting
            print(f"\nFinished sending {count} records to Kafka topic '{topic}'.")

    except FileNotFoundError:
        print(f"Error: CSV file not found at {file_path}")
    except Exception as e:
        print(f"An error occurred during file reading or Kafka sending: {e}")


if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        send_data(kafka_producer, KAFKA_TOPIC, CSV_FILE)
        kafka_producer.close()
        print("Kafka producer closed.")