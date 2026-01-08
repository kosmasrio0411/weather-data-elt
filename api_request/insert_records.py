import psycopg2
import json
import os
from api_request import mock_fetch_data, fetch_data

def connect_to_db():
    print("connecting to the postgresql database...")
    
    db_host = os.getenv('POSTGRES_HOST', 'db') 
    db_name = os.getenv('POSTGRES_DB', 'db')
    db_user = os.getenv('POSTGRES_USER', 'db_user')
    db_pass = os.getenv('POSTGRES_PASSWORD')
    
    if not db_pass:
        print("WARNING: POSTGRES_PASSWORD not found in environment variables!")

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

def create_table(conn):
    print("Creating table if not exists...")
    try:
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
        conn.commit()
        print("Table was created (or already exists)")

    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise

def insert_records(conn, data):
    print("Inserting weather data into database...")
    try:
        weather = data['current']
        location = data['location']
        air_quality_data = weather.get('air_quality', {})
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.raw_weather_data (
                city,
                temperature,
                weather_descriptions,
                time,
                wind_speed,
                wind_degree,
                pressure,
                precipitation,
                humidity,
                cloudcover,
                air_quality,
                inserted_at,
                utc_offset
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
        print("Data successfully inserted")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        raise

def main():
    try:
        data = mock_fetch_data() 
        conn = connect_to_db()
        create_table(conn)
        insert_records(conn, data)
    except Exception as e:
        print(f"An error occurred during execution: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    main()