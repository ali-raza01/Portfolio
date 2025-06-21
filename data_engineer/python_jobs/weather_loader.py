import os
import requests
from snowflake.snowpark import Session
from dotenv import load_dotenv

# Load credentials
load_dotenv()

# --- Snowflake Connection ---
def get_snowflake_session():
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    return Session.builder.configs(connection_parameters).create()

# --- Call OpenWeather API ---
def get_weather_for_city(city):
    api_key = os.getenv("OPENWEATHER_API_KEY")
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed for {city}: {response.status_code}")
        return None

# --- Transform to list of tuples ---
def transform_weather_data(json_data):
    return (
        json_data["name"],
        json_data["main"]["temp"],
        json_data["main"]["humidity"],
        json_data["wind"]["speed"],
        json_data["weather"][0]["description"]
    )

# --- Load into Snowflake ---
def load_to_snowflake(session, rows, table_name="WEATHER_DATA"):
    df = session.create_dataframe(
        rows,
        schema=["CITY", "TEMP_C", "HUMIDITY", "WIND_SPEED", "DESCRIPTION"]
    )
    qualified_table_name = f"{os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.{table_name}"
    df.write.mode("overwrite").save_as_table(qualified_table_name)

    print(f"✅ Loaded weather data for {len(rows)} cities into {table_name}")

# --- Main Execution ---
if __name__ == "__main__":
    cities = ["London", "New York", "Tokyo", "Lahore", "Dubai"]
    weather_rows = []

    for city in cities:
        data = get_weather_for_city(city)
        if data:
            row = transform_weather_data(data)
            print(f"🌤 {row[0]}: {row[1]}°C, {row[4]}")
            weather_rows.append(row)

    if weather_rows:
        session = get_snowflake_session()
        try:
            print("🚀 Loading to Snowflake...")
            load_to_snowflake(session, weather_rows)
            print("✅ Done.")
        except Exception as e:
            print("❌ ERROR:", e)
        finally:
            session.close()