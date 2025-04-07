import json
import boto3
from kafka import KafkaConsumer
from datetime import datetime, timezone
import uuid

bucket_name = 'weather-etl-stream'
prefix = 'weather_data'

consumer = KafkaConsumer(
    'weather',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
s3 = boto3.client('s3')

def convert_to_fahrenheit(celsius):
    return round(celsius * 9 / 5 + 32, 2)

for message in consumer:
    payload = message.value
    weather_entries = payload.get("weather_data")

    if not weather_entries:
        print("No 'weather_data' found in Kafka message")
        continue

    enriched_entries = []

    for entry in weather_entries:
        celsius = entry.get('temperature')
        if celsius is not None:
            entry['temperature_f'] = convert_to_fahrenheit(celsius)

        # Adding additional location metadata
        location_name = entry.get('city', 'Unknown')
        if location_name == 'Berlin':
            entry['location'] = 'Germany'

        elif location_name == 'Paris':
            entry['location'] = 'France'

        elif location_name == 'Madrid': 
            entry['location'] = 'Spain'

        else:
            entry['location'] = 'Unknown'

        enriched_entries.append(entry)

    # Creating batch metadata
    batch_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%S')

    batch_payload = {
        "batch_id": batch_id,
        "timestamp": timestamp,
        "weather_data": enriched_entries
    }

    file_key = f"{prefix}/batch_weather_{timestamp}_{batch_id}.json"


    # Uploads the entire batch as a single JSON array
    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=json.dumps(batch_payload)
    )
    print(f"Uploaded batch: {file_key} — ID: {batch_id}, {len(enriched_entries)} entries")