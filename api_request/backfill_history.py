import psycopg2
import json
import os
import random
from datetime import datetime, timedelta

def connect_to_db():
    print("Connecting to the postgresql database for backfilling...")
    
    db_host = os.getenv('POSTGRES_HOST', 'db') 
    db_name = os.getenv('POSTGRES_DB', 'postgres')
    db_user = os.getenv('POSTGRES_USER', 'postgres')
    db_pass = os.getenv('POSTGRES_PASSWORD', 'postgres')
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=5432,
            dbname=db_name,
            user=db_user,
            password=db_pass
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise

def insert_records(conn, data):
    try:
        weather = data['current']
        location = data['location']
        air_quality_data = weather.get('air_quality', {})
        
        cursor = conn.cursor()

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_descriptions TEXT,
                time TIMESTAMP,
                wind_speed FLOAT,
                wind_degree INT,
                pressure INT,
                precipitation FLOAT,
                humidity INT,
                cloudcover INT,
                air_quality JSONB,
                inserted_at TIMESTAMP DEFAULT NOW(),
                utc_offset TEXT
            );
        """)

        cursor.execute("""
            INSERT INTO dev.raw_weather_data (
                city, temperature, weather_descriptions, time,
                wind_speed, wind_degree, pressure, precipitation,
                humidity, cloudcover, air_quality, inserted_at, utc_offset
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        """,(
            location['name'],
            weather['temperature'],
            weather['weather_descriptions'][0], 
            location['localtime'],
            weather['wind_speed'],
            weather['wind_degree'],
            weather['pressure'],
            weather['precip'],
            weather['humidity'],
            weather['cloudcover'],
            json.dumps(air_quality_data),
            location['utc_offset']
        ))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error inserting backfill data: {e}")
        raise

def run_backfill(days=14):
    conn = None
    try:
        conn = connect_to_db()
        print(f"=== Memulai Backfill Data SIANG HARI untuk {days} hari ke belakang ===")
        
        now = datetime.now()
        
        for i in range(days):
            target_date = (now - timedelta(days=i)).replace(hour=13, minute=0, second=0, microsecond=0)
            
            is_rain = random.random() < 0.4
            
            if is_rain:
                temp = round(random.uniform(26.0, 29.5), 1)
                humidity = random.randint(80, 95)
                precip = round(random.uniform(1.0, 20.0), 1)
                desc_options = ["Light Rain", "Moderate Rain", "Patchy rain nearby"]
                desc = [random.choice(desc_options)]
                cloudcover = random.randint(75, 100)
                uv_index = 2
            else:
                temp = round(random.uniform(30.0, 34.5), 1)
                humidity = random.randint(55, 75)
                precip = 0.0
                desc_options = ["Partly cloudy", "Sunny", "Cloudy"]
                desc = [random.choice(desc_options)]
                cloudcover = random.randint(20, 60)
                uv_index = random.randint(6, 10)
            
            pm_val = round(random.uniform(15.0, 45.0), 2)
            air_quality_mock = {
                "co": f"{round(random.uniform(350.0, 550.0), 2)}",
                "no2": f"{round(random.uniform(3.0, 10.0), 2)}", 
                "o3": f"{round(random.uniform(80.0, 130.0), 2)}",
                "so2": f"{round(random.uniform(5.0, 15.0), 2)}", 
                "pm2_5": f"{pm_val}",
                "pm10": f"{round(pm_val + random.uniform(2.0, 10.0), 2)}",
                "us-epa-index": str(random.randint(1, 2)), 
                "gb-defra-index": str(random.randint(1, 3))
            }

            mock_data = {
                "location": {
                    "name": "Surakarta",
                    "country": "Indonesia",
                    "utc_offset": "7.0",
                    "localtime": target_date.strftime("%Y-%m-%d %H:%M:%S")
                },
                "current": {
                    "temperature": temp,
                    "weather_descriptions": desc,
                    "wind_speed": random.randint(8, 15),
                    "wind_degree": random.randint(0, 360),
                    "pressure": random.randint(1005, 1010),
                    "precip": precip,
                    "humidity": humidity,
                    "cloudcover": cloudcover,
                    "uv_index": uv_index,
                    "air_quality": air_quality_mock
                }
            }
            
            insert_records(conn, mock_data)
            print(f"[SUCCESS] Inserted DAYTIME data for: {target_date.strftime('%Y-%m-%d %H:%M:%S')}")

        print(f"=== Backfill Selesai! Data siang hari telah ditambahkan. ===")
        
    except Exception as e:
        print(f"An error occurred during backfill: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    run_backfill(days=14)