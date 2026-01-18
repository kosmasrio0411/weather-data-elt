import os
import requests

api_key = os.getenv('WEATHER_API_KEY')

api_url = f"https://api.weatherstack.com/current?access_key={api_key}&query=Yogyakarta"

def fetch_data():
    print("Fetching weather data from weatherstack API...")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        print("API response received succesfully.")
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"An error occured: {e}")
        raise

# fetch_data()

def mock_fetch_data():
    return {"request":{"type":"City","query":"Yogyakarta, Indonesia","language":"en","unit":"m"},"location":{"name":"Yogyakarta","country":"Indonesia","region":"Daerah Istimewa Yogyakarta","lat":"-7.783","lon":"110.361","timezone_id":"Asia\/Jakarta","localtime":"2026-01-17 20:15","localtime_epoch":1768680900,"utc_offset":"7.0"},"current":{"observation_time":"01:15 PM","temperature":24,"weather_code":293,"weather_icons":["https:\/\/cdn.worldweatheronline.com\/images\/wsymbols01_png_64\/wsymbol_0033_cloudy_with_light_rain_night.png"],"weather_descriptions":["Patchy light rain"],"astro":{"sunrise":"05:33 AM","sunset":"06:04 PM","moonrise":"03:58 AM","moonset":"04:57 PM","moon_phase":"Waning Crescent","moon_illumination":3},"air_quality":{"co":"440.85","no2":"5.15","o3":"104","so2":"8.05","pm2_5":"22.65","pm10":"22.65","us-epa-index":"2","gb-defra-index":"2"},"wind_speed":7,"wind_degree":258,"wind_dir":"WSW","pressure":1007,"precip":1.8,"humidity":93,"cloudcover":66,"feelslike":26,"uv_index":0,"visibility":9,"is_day":"no"}}