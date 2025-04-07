# Real-Time Weather Streaming ETL Pipeline with Kafka & AWS
This project demonstrates a real-time **streaming ETL pipeline** built with **Apache Kafka**, **Python**, and **AWS S3**, using live weather data from the **Open-Meteo API**. It simulates a production-grade data ingestion flow with batching, transformation, and cloud storage — all using services available on the AWS Free Tier.

---

## Architecture Overview
[Open-Meteo API] → [Kafka Producer] → [Kafka Topic] → [Kafka Consumer] → [Transformation Layer] → [Amazon S3]
- **Kafka Producer**: Fetches real-time weather data for desired cities.
- **Kafka Consumer**: Transforms data (adds metadata like country and converts temperature to °F) and pushes to AWS S3 in JSON format.
- **AWS S3**: Serves as a data lake for storing JSON files.
- **Further Improvements**: Designed for future integration with Amazon QuickSight or Athena.

---

## Technologies Used

- **Apache Kafka (Docker)**
- **Python 3.13**
  - `kafka-python`
  - `requests`
  - `boto3`
- **AWS S3** (Free Tier)
- **Open-Meteo API** (Free, no auth)
- **UUID** (for traceability of each push to S3)

---

## Cities Tracked

- Berlin, Germany  
- Paris, France  
- Madrid, Spain  

Each city is fetched individually and included in a batched JSON file with location-specific metadata.

---

## 🔄 ETL Features

- ✅ **Real-time ingestion** (every 10 seconds)
- ✅ **Batching by timestamp**
- ✅ **Transformation**:
  - Celsius → Fahrenheit
  - City → Country mapping
  - Enriched with batch UUID and ISO timestamp

---

## 📁 Example Output (JSON file stored in S3)

```json
{
  "batch_id": "8f210b45-68d5-4c91-9f2d-b1e741ccba88",
  "timestamp": "2025-04-07T14-30-01",
  "weather_data": [
    {
      "time": "2025-04-07T14:30",
      "temperature": 13.2,
      "temperature_f": 55.76,
      "city": "Berlin",
      "location": "Germany",
      "windspeed": 14.1
    },
    ...
  ]
}
```
## How to Run Locally
- Start Kafka on Docker
- Start the Producer
- Start the Consumer in another terminal (switch the bucket_name with your own)

## AWS Notes
- Data is pushed to S3 using boto3 and access keys configured via aws configure.

## Screenshot of S3 Bucket
<img width="1433" alt="Screenshot 2025-04-07 at 18 37 46" src="https://github.com/user-attachments/assets/7acd4b8c-2142-46f3-b83b-50bfaaa12aad" />
