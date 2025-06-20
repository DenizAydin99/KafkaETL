import json
import boto3
from kafka import KafkaConsumer
from datetime import datetime, timezone
import uuid
import botocore.exceptions
import time

bucket_name = 'weather-etl-stream'
prefix = 'weather_data'

consumer = KafkaConsumer(
    'weather',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
s3 = boto3.client('s3')

# Hardcoded mapping of city names to countries, countries are not delivered by the API
COUNTRY_BY_CITY = {
    'Berlin': 'Germany',
    'Paris': 'France',
    'Madrid': 'Spain'
}

def convert_to_fahrenheit(celsius):
    return round(celsius * 9 / 5 + 32, 2)

for message in consumer:
    payload = message.value
    weather_entries = payload.get("weather_data")

    if not weather_entries:
        print("No 'weather_data' found in Kafka message")
        continue

    enriched_entries = []

    # Adding fahrenheit conversion
    for entry in weather_entries:
        celsius = entry.get('temperature')
        if celsius is not None:
            entry['temperature_f'] = convert_to_fahrenheit(celsius)

        # Adding additional location metadata for each city
        location_name = entry.get('city')
        
        # Defaults to unknown if city is not in the mapping
        entry['location'] = COUNTRY_BY_CITY.get(location_name,"Unknown")

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

    # Uploads the entire batch as a single JSON file
    for attempt in range(1, 4):
        try:
            s3.put_object(Bucket=bucket_name, Key=file_key,
                        Body=json.dumps(batch_payload))
            print(f"Uploaded {file_key} ({len(enriched_entries)} records)")
            break
        except botocore.exceptions.ClientError as e:
            print(f"Upload attempt {attempt} failed: {e}")
            time.sleep(2 ** attempt)          # 2 s, 4 s, 8 s
    else:
        print(f"Permanent S3 failure for {file_key}")
        

    #print(f"Uploaded batch: {file_key} — ID: {batch_id}, {len(enriched_entries)} entries")