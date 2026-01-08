import os

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
    return {"request":{"type":"City","query":"Yogyakarta, Indonesia","language":"en","unit":"m"},"location":{"name":"Yogyakarta","country":"Indonesia","region":"Daerah Istimewa Yogyakarta","lat":"-7.783","lon":"110.361","timezone_id":"Asia\/Jakarta","localtime":"2026-01-06 14:08","localtime_epoch":1767708480,"utc_offset":"7.0"},"current":{"observation_time":"07:08 AM","temperature":31,"weather_code":116,"weather_icons":["https:\/\/cdn.worldweatheronline.com\/images\/wsymbols01_png_64\/wsymbol_0002_sunny_intervals.png"],"weather_descriptions":["Partly Cloudy "],"astro":{"sunrise":"05:28 AM","sunset":"06:01 PM","moonrise":"08:45 PM","moonset":"08:02 AM","moon_phase":"Waning Gibbous","moon_illumination":91},"air_quality":{"co":"1102.85","no2":"32.25","o3":"31","so2":"10.35","pm2_5":"79.35","pm10":"80.75","us-epa-index":"4","gb-defra-index":"4"},"wind_speed":16,"wind_degree":227,"wind_dir":"SW","pressure":1010,"precip":0,"humidity":53,"cloudcover":46,"feelslike":34,"uv_index":8,"visibility":10,"is_day":"yes"}}