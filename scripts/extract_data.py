
from datetime import datetime, timedelta, UTC # Importing datetime and timedelta modules/class for scheduling the DAGs
# import airflow
# Importing operators 
# https://medium.com/data-science/mastering-airflow-variables-32548a53b3c5
# from airflow.models import Variable

import requests # Python Requests library for making HTTP requests to a retrieve our data from a specified URL
import pandas as pd # pandas library necessary for creating and maniplulating dataframes
import json
from dotenv import load_dotenv
import os
load_dotenv()
# https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}
# api_params = {
#         "q": "Nairobi,Kenya",
#         "appid": Variable.get("api_key")
#     }

api_endpoint = "https://api.openweathermap.org/data/2.5/weather"

cities = [
    {"name": "Kampala", "country": "UG"},
    {"name": "Kigali", "country": "RW"},
    {"name": "Nairobi", "country": "KE"},
    {"name": "Zurich", "country": "CH"},
    {"name": "Warsaw", "country": "PL"}
]
def extract_openweather_data():
    weather_list = []
    for city in cities:
        api_params = {
            'q': f"{city['name']},{city['country']}",
            'appid': os.getenv("api_key")
        }
        response = requests.get(api_endpoint, params=api_params) # python requests using get method to retrieve info from openweather api
        if response.status_code == 200:
            data = response.json() # returns a JSON object of the response returned from a request
            weather_list.append({
                'weather_description': data['weather'][0]['description'],
                'city_name': city['name'],
                'country': city['country'],
                'latitude': data['coord']['lat'],
                'longitude': data['coord']['lon'],
                'temperature': data['main']['temp'],
                'feels_like_farenheit': data["main"]["feels_like"],
                'min_temp_fahrenheit': data["main"]["temp_min"],
                'max_temp_farenheit': data["main"]["temp_max"],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'wind_speed': data['wind']['speed'],
                'datetime': datetime.fromtimestamp(data['dt']).isoformat()
                  })
        else:
            print(f"Error fetching data for {data['name']}: {response.status_code}")
    return weather_list

if __name__ == "__main__": # running this python file as a standalone script, such that when we import  
    data = extract_openweather_data()
    for item in data:
        print(item)