import requests
import json
import time
from kafka import KafkaProducer

cities = {
    "Berlin": (52.52, 13.41),
    "Paris": (48.85, 2.35),
    "Madrid": (40.42, -3.70)
}

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    enriched_data = []

    for city_name, (lat, lon) in cities.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                weather = response.json().get("current_weather")
                if weather:
                    # Appending additional metadata to the API response
                    weather["city"] = city_name
                    weather["latitude"] = lat
                    weather["longitude"] = lon
                    enriched_data.append(weather)
                else:
                    print(f"No weather data for {city_name}")
            else:
                print(f"Error fetching data for {city_name}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Exception fetching {city_name}: {e}")

    if enriched_data:
        # Send all city data as one Kafka message
        producer.send('weather', value={"weather_data": enriched_data})
        print(f"Sent weather batch for {len(enriched_data)} cities")
    else:
        print("No data sent this cycle")

    time.sleep(20)