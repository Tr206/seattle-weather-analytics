import os
import requests
import time
from datetime import datetime, timedelta, UTC
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# 1. Configuration: Add your states/cities here
CITIES = [
    {"name": "Seattle", "lat": 47.6062, "lon": -122.3321},
    {"name": "Denver", "lat": 39.7392, "lon": -104.9903},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437}
]

def run_weather_pipeline():
    API_KEY = os.getenv("WEATHER_API_KEY")
    PROJECT_ID = os.getenv("GCP_PROJECT_ID", "seattle-weather-analytics")
    DATASET = "weather_raw"
    TABLE = "seattle_weather_history"
    FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    
    client = bigquery.Client(project=PROJECT_ID)

    for city in CITIES:
        print(f"--- Processing {city['name']} ---")
        all_rows = []
        
        # Pulling last 30 days
        for i in range(0, 31):
            target_date = datetime.now(UTC) - timedelta(days=i)
            unix_time = int(target_date.timestamp())
            
            url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={city['lat']}&lon={city['lon']}&dt={unix_time}&appid={API_KEY}&units=imperial"
            
            try:
                response = requests.get(url).json()
                if "data" in response:
                    d = response["data"][0]
                    all_rows.append({
                        "timestamp": datetime.fromtimestamp(d["dt"], UTC).strftime('%Y-%m-%d %H:%M:%S'),
                        "city": city['name'],
                        "temp": d["temp"],
                        "humidity": d["humidity"],
                        "weather_description": d["weather"][0]["description"],
                        "wind_speed": d.get("wind_speed", 0)
                    })
            except Exception as e:
                print(f"Error fetching {city['name']} for day {i}: {e}")
            
            time.sleep(0.1) # Respecting rate limits

        # 2. The "Anti-Duplicate" Merge Strategy
        if all_rows:
            # Create a temporary staging table for just this batch
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            staging_table = f"{PROJECT_ID}.{DATASET}.temp_stage_{city['name'].replace(' ', '_')}"
            client.load_table_from_json(all_rows, staging_table, job_config=job_config).result()

            # MERGE staging table into main table (Only insert if timestamp + city is new)
            merge_query = f"""
            MERGE `{FULL_TABLE_ID}` T
            USING `{staging_table}` S
            ON T.timestamp = S.timestamp AND T.city = S.city
            WHEN NOT MATCHED THEN
            INSERT (timestamp, city, temp, humidity, weather_description, wind_speed, ingestion_timestamp)
            VALUES (timestamp, city, temp, humidity, weather_description, wind_speed, CURRENT_TIMESTAMP())
            """
            client.query(merge_query).result()
            print(f"✅ Successfully synced unique rows for {city['name']}.")
            
            # Clean up staging table
            client.delete_table(staging_table, not_found_ok=True)

if __name__ == "__main__":
    run_weather_pipeline()