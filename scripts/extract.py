import requests

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



