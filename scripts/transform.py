"""
===============================================================
transform.py

Fonctions pour nettoyer et transformer les données météo
(actuelles, prévisions et historiques).

Principales fonctions :

1 clean_weather_data(weather_data)
   - Nettoie et agrège les données météo OpenWeather.
   - Retour : DataFrame journalier avec température, vent, pluie, score.

2 calculate_score(temp, rain, wind)
   - Calcule un score météo sur 20 pour données actuelles/prévisions.

3 calculate_score_historique(temp, rain, wind)
   - Calcule un score météo sur 20 pour données historiques.

4 clean_weather_data_meteostat(df)
   - Transforme et agrège les données Meteostat historiques.
   - Retour : DataFrame journalier nettoyé.

5 transform_to_star(data_dir)
   - Crée un schéma en étoile (City, Date, Weather) pour Data Warehouse.
   - Retour : dossier contenant les fichiers générés.

===============================================================
"""

import pandas as pd
from datetime import timedelta
import os

# Traitement des données actuelles et prévisions sur 5 jours
def clean_weather_data(weather_data):
    rows = []
    for entry in weather_data['list']:
        temp = entry['main']['temp']
        wind = entry['wind']['speed']
        rain = entry.get('rain', {}).get('3h', 0)
        date = entry['dt_txt'].split()[0]
        score = calculate_score(temp, rain, wind)
        rows.append({"Date": date, "Temperature": temp, "Wind_Speed": wind, "Rain": rain, "Score": score})

    df = pd.DataFrame(rows)
    df = df.groupby("Date").agg({
        "Temperature": "mean",
        "Wind_Speed": "mean",
        "Rain": "sum",
        "Score": "mean"
    }).reset_index()
    
    df = df.round(1)
    df["Temperature"] = df["Temperature"].round(0).astype(int)
    df["Wind_Speed"] = df["Wind_Speed"].round(0).astype(int)
    df["Rain"] = df["Rain"].round(0).astype(int)
    df["Score"] = df["Score"].round(0).astype(int)
    
    return df

# Score météo sur 20 pour données actuelles et prévisions 5 jours 
def calculate_score(temp, rain, wind):
    score = 0

    # Température (max 10 points)
    if 22 <= temp <= 28:
        score += 10
    elif 20 <= temp < 22 or 28 < temp <= 30:
        score += 8
    elif 17 <= temp < 20 or 30 < temp <= 33:
        score += 6
    elif 11 <= temp < 17 or 33 < temp <= 35:
        score += 4
    else: 
        score += 2

    # Pluie (max 5 points)
    if rain == 0:
            score += 5
    elif rain < 2:
        score += 3
    elif rain < 5:
        score += 2
    elif rain < 10:
        score += 1
    else: 
        score += 0
    # Vent (max 5 points)
    if wind < 2:
        score += 5
    elif wind < 4:
        score += 3
    elif wind < 6:
        score += 2
    elif wind < 10:
        score += 1
    else:   
        score += 0
    
    return min(score, 20)  # Cap à 20

# Score météo sur 20 pour données historiques
def calculate_score_historique(temp, rain, wind):
    score = 0

    if pd.notna(temp):
        if 22 <= temp <= 28:
            score += 10
        elif 20 <= temp < 22 or 28 < temp <= 30:
            score += 8
        elif 18 <= temp < 20 or 30 < temp <= 32:
            score += 6
        elif 11 <= temp < 18 or 32 < temp <= 35:
            score += 4
        else: 
            score += 2

    if pd.notna(rain):
        if rain == 0:
            score += 5
        elif rain < 2:
            score += 3
        elif rain < 5:
            score += 2
        elif rain < 10:
            score += 1
        else: 
            score += 0

    if pd.notna(wind):
        if wind < 2:
            score += 5
        elif wind < 4:
            score += 3
        elif wind < 6:
            score += 2
        elif wind < 10:
            score += 1
        else:   
            score += 0

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
                "Date": base_date.date(),
                "Heure": heure,
                "Temperature": temp,
                "Wind_Speed": wind,
                "Rain": rain_3h,
                "Score": score
            })

    df_3h = pd.DataFrame(rows)
        # Regroupement final par date (moyenne ou somme selon l'indicateur)
    df_daily = df_3h.groupby("Date").agg({
        "Temperature": "mean",
        "Wind_Speed": "mean",
        "Rain": "sum",
        "Score": "mean"
    }).reset_index()
    
    df_daily["Temperature"] = df_daily["Temperature"].round(0).astype(int)
    df_daily["Wind_Speed"] = df_daily["Wind_Speed"].round(0).astype(int)
    df_daily["Rain"] = df_daily["Rain"].round(0).astype(int)
    df_daily["Score"] = df_daily["Score"].round(0).astype(int)

    return df_daily


def transform_to_star(data_dir="airflow/dags/climat_tourisme/data"):
    """
    Construit les fichier City.csv, Date.csv et Meteo.csv
    à partir des fichiers weather_combined_<city>.csv
    """
    output_dir = os.path.join(data_dir, "star_schema")
    os.makedirs(output_dir, exist_ok=True)

    # --- 1) Création de la table dimension City ---
    cities = []
    files = [f for f in os.listdir(data_dir) if f.startswith("weather_combined_")]
    for i, file in enumerate(files, start=1):
        city_nom = file.replace("weather_combined_", "").replace(".csv", "")
        cities.append({"City_Id": i, "City_Name": city_nom})
    df_city = pd.DataFrame(cities)
    df_city.to_csv(os.path.join(output_dir, "City.csv"), index=False)

    # --- 2) Création de la table fait Weather ---
    fact_rows = []
    for i, file in enumerate(files, start=1):
        city_id = i
        df = pd.read_csv(os.path.join(data_dir, file))
        
        df['Date_Id'] = pd.to_datetime(df['Date']).dt.strftime('%Y%m%d').astype(int)
        df["City_Id"] = city_id
        
        if 'Date' in df.columns:
            df = df.drop(columns=['Date'])
        
        df["Temperature"] = df["Temperature"].round(0).astype(int)
        df["Wind_Speed"] = df["Wind_Speed"].round(0).astype(int)
        df["Rain"] = df["Rain"].round(0).astype(int)
        df["Score"] = df["Score"].round(0).astype(int)
        
        columns = ["Date_Id", "City_Id"] + [col for col in df.columns if col not in ["Date_Id", "City_Id"]]
        df = df[columns]
        
        fact_rows.append(df)
    df_fact = pd.concat(fact_rows, ignore_index=True)
    df_fact.to_csv(os.path.join(output_dir, "Weather.csv"), index=False)

    # --- 3) Création de la table dimension Date ---
    all_dates = pd.to_datetime(df_fact["Date_Id"].unique(), format="%Y%m%d")
    dim_date_rows = []
    for dt in all_dates:
        dim_date_rows.append({
            "Date_Id": dt.strftime("%Y%m%d"),
            "Date": dt.strftime("%Y-%m-%d"),
            "Day": dt.day,
            "Month": dt.month,
            "Year": dt.year,
            "Day_Week": dt.strftime("%A"),
            "Week_Number": dt.isocalendar()[1]
        })
    df_dim_date = pd.DataFrame(dim_date_rows)
    df_dim_date.to_csv(os.path.join(output_dir, "Date.csv"), index=False)

    print(f"[✓] Schéma étoile généré : {output_dir}")
    return output_dir
