import pandas as pd

# actuel et prevision dans 5 jour 
 
def clean_weather_data(weather_data):
    rows = []
    for entry in weather_data['list']:
        temp = entry['main']['temp']
        wind = entry['wind']['speed']
        rain = entry.get('rain', {}).get('3h', 0)
        date = entry['dt_txt'].split()[0]
        score = calculate_score(temp, rain, wind)
        rows.append({"date": date, "temp": temp, "wind": wind, "rain": rain, "score": score})

    df = pd.DataFrame(rows)
    df = df.groupby("date").agg({
        "temp": "mean",
        "wind": "mean",
        "rain": "sum",
        "score": "sum"
    }).reset_index()
    return df

def calculate_score(temp, rain, wind):
    score = 0
    if 22 <= temp <= 28:
        score += 1
    if rain == 0:
        score += 1
    if wind < 4:
        score += 1
    return score

import math

# historique

def calculate_score_historique(temp, rain, wind):
    score = 0
    if pd.notna(temp) and 22 <= temp <= 28:
        score += 1
    if pd.notna(rain) and rain == 0:
        score += 1
    if pd.notna(wind) and wind < 4:
        score += 1
    return score

def clean_weather_data_meteostat(df):
    df["score"] = df.apply(lambda row: calculate_score_historique(row["temp"], row["rain"], row["wind"]), axis=1)
    return df