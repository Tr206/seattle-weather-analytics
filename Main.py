import os
import requests
from google.cloud import bigquery
from datetime import datetime
import functions_framework

@functions_framework.http
def get_weather(request):
    # Securely get API key from environment variables (configured in Google Cloud)
    API_KEY = os.getenv("WEATHER_API_KEY")
    CITY = "Seattle"
    PROJECT_ID = "seattle-weather-analytics"
    TABLE_ID = f"{PROJECT_ID}.weather_raw.seattle_weather_history"

    # API Request
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=imperial"
    response = requests.get(url).json()

    # Data Transformation: Preparing rows for BigQuery
    rows_to_insert = [{
        "timestamp": datetime.utcnow().isoformat(),
        "city": response["name"],
        "temp": response["main"]["temp"],
        "humidity": response["main"]["humidity"],
        "weather_description": response["weather"][0]["description"],
        "wind_speed": response["wind"]["speed"]
    }]

    # Load into BigQuery
    client = bigquery.Client()
    errors = client.insert_rows_json(TABLE_ID, rows_to_insert)

    if not errors:
        return f"Successfully inserted weather data for {CITY}.", 200
    else:
        return f"BigQuery Insert Errors: {errors}", 500