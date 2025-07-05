import requests
import pandas as pd
from meteostat import Point, Daily

# APIs OpenWeather
def get_city_coordinates(city, api_key):
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={api_key}"
    response = requests.get(url)
    data = response.json()
    return data[0]["lat"], data[0]["lon"]

# prévisions 5 jours par tranches de 3h
def get_weather_forecast(city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&units=metric&appid={api_key}"
    response = requests.get(url)
    return response.json()


# Récupération historique avec Meteostat
def get_historical_weather_meteostat(city, start, end):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="weather_etl")
    location = geolocator.geocode(city, timeout=10)
    if not location:
        raise ValueError("Ville non trouvée")
    
    point = Point(location.latitude, location.longitude)
    data = Daily(point, start, end)
    df = data.fetch()
    df.reset_index(inplace=True)
    df["date"] = df["time"].dt.date
    df = df[["date", "tavg", "wspd", "prcp"]]  # moyenne T, vitesse vent, précipitations
    df.columns = ["date", "temp", "wind", "rain"]
    return df
