import requests
import json
import time
import pandas as pd
from kafka import KafkaProducer
import threading
from datetime import datetime, timezone
import zoneinfo

# Hardcoding city names and coordinates to easily test the code
cities = {
    "Berlin": (52.52, 13.41),
    "Paris": (48.85, 2.35),
    "Madrid": (40.42, -3.70)
}

# Mapping from city to timezone
TZ_BY_CITY = {
    "Berlin": zoneinfo.ZoneInfo("Europe/Berlin"),
    "Paris": zoneinfo.ZoneInfo("Europe/Paris"),
    "Madrid": zoneinfo.ZoneInfo("Europe/Madrid"),
}

enriched_data = []

data_lock = threading.Lock()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Flags to indicate if the API and CSV data retrieval processes are finished
api_finished = False
csv_finished = False

stop_event = threading.Event()

def get_data_from_api():
    global api_finished, enriched_data
    
    while not stop_event.is_set():

        api_finished = False
        print("API Thread: Flag set to False")

        for city_name, (lat, lon) in cities.items():
            url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
            
            try:
                response = requests.get(url, timeout=5)
                
                if response.status_code == 200:
                    weather = response.json().get("current_weather")

                    if weather:
                        # Appending additional metadata to the API response
                        if "city" not in weather:
                            weather["city"] = city_name

                        if "latitude" not in weather:
                            weather["latitude"] = lat
                        
                        if "longitude" not in weather:
                            weather["longitude"] = lon

                        with data_lock:
                            enriched_data.append(weather)
                            print(f"API Thread: Appended {city_name} to enriched_data")

                    else:
                        print(f"API Thread: No weather data received for {city_name}")
                else:
                    print(f"API Thread: Error while fetching data for {city_name}: {response.status_code}")

            except requests.exceptions.RequestException as e:
                print(f"API Thread: API exception fetching for {city_name}: {e}")            

        api_finished = True   
        print("API Thread: Flag set to True")
        time.sleep(20) # Using a buffer of 20 seconds to avoid making too many requests while testing


def get_data_from_csv():
    global csv_finished, enriched_data

    df = pd.read_csv("weather_data.csv")
    print(f"CSV loaded: {len(df)} rows")

    row_index = 0  # Tracks the next row to process
    batch_size = 3 # Number of rows to process in each cycle
    
    while row_index < len(df) and not stop_event.is_set():
        csv_finished = False
        print("CSV Thread: Flag set to False")

        # Get the next 3 rows (or fewer if at the end of the DataFrame)
        next_rows = df.iloc[row_index : row_index + batch_size]

        appended_count = 0 # count of appended rows to enriched_data
        
        for _,row in next_rows.iterrows():
            try:
                csv_enriched_data = {
                    "time": row["time"],
                    "interval": row["interval"],
                    "temperature": row["temperature"],
                    "windspeed": row["windspeed"],
                    "winddirection": row["winddirection"],
                    "is_day": row["is_day"],
                    "weathercode": row["weathercode"],
                    "city": row["city"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "temperature_f": row["temperature_f"],
                    "location": row["location"]
                }
            
                with data_lock:
                    enriched_data.append(csv_enriched_data)
                    appended_count += 1
                    print(f"CSV Thread: Appended {csv_enriched_data['city']} to enriched_data")

            except Exception as e:
                print(f"CSV Thread: Exception at row {row_index + appended_count}: {e}")

        print(f"CSV thread: appended {appended_count} rows of data to enriched_data")

        row_index += len(next_rows)  # Moves to the next batch of rows
        
        if row_index >= len(df):
            csv_finished = True
            print("CSV Thread: Reached end of file and flag set to True")
            break

        csv_finished = True
        print("CSV Thread: Finished processing the batch, flag set to true.")

        time.sleep(20)

# Converts a local timestamp string and city to a UTC timestamp string
def convert_to_utc(local_ts: str, city: str) -> str:
    """
    Converts a local timestamp to a UTC timestamp string.
    """
    naive = datetime.strptime(local_ts, "%Y-%m-%dT%H:%M")  # no tzinfo
    aware_local = naive.replace(tzinfo=TZ_BY_CITY[city])   # tag zone
    aware_utc = aware_local.astimezone(timezone.utc)       # conversion to UTC
    return aware_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

# Appends a UTC timestamp to each entry in enriched_data
def enrich_timestamp(enriched_data):
    """
    Input  : '2025-04-07T15:00', 'Berlin'
    Output : ('2025-04-07T15:00', '2025-04-07T13:00:00Z')
    """

    for entry in enriched_data:
        local_ts = entry.get("time")
        city = entry.get("city")

        if not local_ts or not city:
            print(f"Skipping entry due to missing time or city: {entry}")
            continue

        try:
            utc_ts = convert_to_utc(local_ts, city)
            entry["time_utc"] = utc_ts
        except Exception as e:
            print(f"Error converting time for {city}: {e}")


def main():
    global enriched_data, csv_finished, api_finished
    csv_thread = threading.Thread(target=get_data_from_csv)
    api_thread = threading.Thread(target=get_data_from_api)

    csv_thread.start()
    api_thread.start()

    try:
        while True:
            time.sleep(5)  # Avoids busy-waiting by checking flags every 5 seconds
            
            if csv_finished and api_finished:
                
                with data_lock:
                    if not enriched_data:          # list is empty
                        api_finished = False       # wait for next API batch
                        continue                   # skip the rest of the loop

                print("Both sources finished their cycle, pushing data to Kafka consumer...")
                
                try:
                    enrich_timestamp(enriched_data)  # Appends UTC timestamps before sending    
                    producer.send('weather', value={"weather_data": enriched_data})
                    producer.flush()  # Ensures all messages are sent by forcing the producer to send all buffered messages
                    
                    size = len(enriched_data)
                    print(f"Data for {size} cities sent to Kafka consumer successfully.")
                    
                    with data_lock:
                        enriched_data.clear()
                    
                    # We only need to reset API flag as the CSV extraction stops after the last batch
                    api_finished = False

                except Exception as e:
                    print(f"Error sending data to Kafka consumer: {e}")

            else:
                print("Waiting for both sources to finish their cycle...")
    
    except KeyboardInterrupt:
        print("Stopping threads... flushing producer")
        stop_event.set()                       
        csv_thread.join(timeout=5)             
        api_thread.join(timeout=5)
        producer.flush()
        producer.close()
        return
    
if __name__ == "__main__":
    main()