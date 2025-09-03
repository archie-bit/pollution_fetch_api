import requests
import json
import time
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
# print(API_KEY)
if not API_KEY:
    raise RuntimeError("OPENWEATHER_API_KEY is not set. Put it in dags/.env")

city_dict= [{"name": "cairo"}, {"name":"warsaw"}, {"name":"beijing"}]

DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

WEATHER_FILE = os.path.join(DATA_DIR, "weather_data.json")
POLLUTION_FILE = os.path.join(DATA_DIR, "pollution_data.json")

# print(f"Writing weather data to {WEATHER_FILE}")

def get_geo(city_dict):
    mydict={}
    for city in city_dict:
        city_r = requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={city['name']}&appid={API_KEY}")
        city_r.raise_for_status() 
        data= city_r.json()
        if data:
            city.update({
                "lat": data[0].get("lat"),
                "lon": data[0].get("lon")
            })
        mydict[city['name']] = city
        time.sleep(.2)
        # print(mydict)
    return mydict

def get_pollution(city_dict):
    cities = get_geo(city_dict)
    # cities= {'cairo': {'name': 'cairo', 'lat': 30.0443879, 'lon': 31.2357257}, 'warsaw': {'name': 'warsaw', 'lat': 52.2319581, 'lon': 21.0067249}, 'beijing': {'name': 'beijing', 'lat': 39.906217, 'lon': 116.3912757}}
    for city in cities.values():
    
        r_weather= requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}")
        r_pollution= requests.get(f"http://api.openweathermap.org/data/2.5/air_pollution?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}")

        if (r_pollution.status_code==200 and r_weather.status_code==200):
                       
            try:
                with open(WEATHER_FILE, 'r+') as file:
                    try:
                        file_data = json.load(file)
                    except json.JSONDecodeError:
                        file_data = {}
                    city_name = city['name']
                    new_entry = {
                        "timestamp": datetime.now().isoformat(),
                        "data": r_weather.json()
                    }

                    # Initialize list if city not present
                    if city_name not in file_data:
                        file_data[city_name] = []

                    # Append new entry
                    file_data[city_name].append(new_entry)

                    # Write back to file
                    file.seek(0)
                    file.truncate()
                    json.dump(file_data, file, indent=4)
            except FileNotFoundError:
                    with open(WEATHER_FILE, 'w') as file:
                        weather_json = {
                            city['name']: [
                                {
                                    "timestamp": datetime.now().isoformat(),
                                    "data": r_weather.json()
                                }
                            ]
                        }
                        json.dump(weather_json, file, indent=4)


            #Pollution        
            try:
                with open(POLLUTION_FILE, 'r+') as file:
                    try:
                        file_data = json.load(file)
                    except json.JSONDecodeError:
                        file_data = {}
                    city_name = city['name']
                    new_entry = {
                        "timestamp": datetime.now().isoformat(),
                        "data": r_pollution.json()
                    }

                    # Initialize list if city not present
                    if city_name not in file_data:
                        file_data[city_name] = []

                    # Append new entry
                    file_data[city_name].append(new_entry)

                    # Write back to file
                    file.seek(0)
                    file.truncate()
                    json.dump(file_data, file, indent=4)
            except FileNotFoundError:
                with open(POLLUTION_FILE, 'w') as file:
                    pollution_json = {
                        city['name']: [
                            {
                                "timestamp": datetime.now().isoformat(),
                                "data": r_pollution.json()
                            }
                        ]
                    }
                    json.dump(pollution_json, file, indent=4)


get_pollution(city_dict)