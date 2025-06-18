import pandas as pd

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
