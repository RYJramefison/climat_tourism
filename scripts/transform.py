
import pandas as pd
from datetime import timedelta

# Traitement des données actuelles et prévisions sur 5 jours
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
        "score": "mean"  # score moyen sur 20
    }).reset_index()
    return df

# Score météo sur 20 pour données actuelles
def calculate_score(temp, rain, wind):
    score = 0

    # Température (max 10 points)
    if 22 <= temp <= 28:
        score += 10
    elif 20 <= temp < 22 or 28 < temp <= 30:
        score += 7
    elif 18 <= temp < 20 or 30 < temp <= 32:
        score += 5

    # Pluie (max 5 points)
    if rain == 0:
        score += 5
    elif rain < 2:
        score += 3
    elif rain < 5:
        score += 1

    # Vent (max 5 points)
    if wind < 2:
        score += 5
    elif wind < 4:
        score += 3
    elif wind < 6:
        score += 1

    return min(score, 20)  # Cap à 20

# Score météo sur 20 pour données historiques
def calculate_score_historique(temp, rain, wind):
    score = 0

    if pd.notna(temp):
        if 22 <= temp <= 28:
            score += 10
        elif 20 <= temp < 22 or 28 < temp <= 30:
            score += 7
        elif 18 <= temp < 20 or 30 < temp <= 32:
            score += 5

    if pd.notna(rain):
        if rain == 0:
            score += 5
        elif rain < 2:
            score += 3
        elif rain < 5:
            score += 1

    if pd.notna(wind):
        if wind < 2:
            score += 5
        elif wind < 4:
            score += 3
        elif wind < 6:
            score += 1

    return min(score, 20)

# Nettoyage des données historiques avec score météo sur 20
def clean_weather_data_meteostat(df):
    rows = []

    for _, row in df.iterrows():
        base_date = pd.to_datetime(row["date"])
        temp = row["temp"]
        wind = row["wind"]
        rain = row["rain"]

        rain_3h = (rain / 8) if pd.notna(rain) else 0

        for i in range(8):  # chaque tranche de 3h
            dt = base_date + timedelta(hours=i * 3)
            heure = dt.strftime("%H:%M")
            score = calculate_score_historique(temp, rain_3h, wind)

            rows.append({
                "datetime": dt,
                "date": base_date.date(),
                "heure": heure,
                "temp": temp,
                "wind": wind,
                "rain": rain_3h,
                "score": score
            })

    df_3h = pd.DataFrame(rows)
        # Regroupement final par date (moyenne ou somme selon l'indicateur)
    df_daily = df_3h.groupby("date").agg({
        "temp": "mean",
        "wind": "mean",
        "rain": "sum",
        "score": "mean"
    }).reset_index()

    return df_daily
