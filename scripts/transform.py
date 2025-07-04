
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
        rows.append({"date": date, "temp": temp, "wind": wind, "rain": rain, "score": score})

    df = pd.DataFrame(rows)
    df = df.groupby("date").agg({
        "temp": "mean",
        "wind": "mean",
        "rain": "sum",
        "score": "mean"  # score moyen sur 20
    }).reset_index()
    return round(df)

# Score météo sur 20 pour données actuelles
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

    return round(df_daily)

def transform_to_star(data_dir="airflow/dags/climat_tourisme/data"):
    """
    Construit dim_ville.csv, dim_date.csv et fact_meteo.csv
    à partir des fichiers weather_combined_<ville>.csv
    """
    output_dir = os.path.join(data_dir, "star_schema")
    os.makedirs(output_dir, exist_ok=True)

    # --- 1) Crée la dimension ville ---
    villes = []
    files = [f for f in os.listdir(data_dir) if f.startswith("weather_combined_")]
    for i, file in enumerate(files, start=1):
        ville_nom = file.replace("weather_combined_", "").replace(".csv", "")
        villes.append({"ville_id": i, "ville_nom": ville_nom})
    df_dim_ville = pd.DataFrame(villes)
    df_dim_ville.to_csv(os.path.join(output_dir, "dim_ville.csv"), index=False)

    # --- 2) Crée la fact table ---
    fact_rows = []
    for i, file in enumerate(files, start=1):
        ville_id = i
        df = pd.read_csv(os.path.join(data_dir, file))
        
        df['date_id'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d').astype(int)
        df["ville_id"] = ville_id
        
        columns = ["date_id", "ville_id"] + [col for col in df.columns if col not in ["date_id", "ville_id"]]
        df = df[columns]
        
        fact_rows.append(df)
    df_fact = pd.concat(fact_rows, ignore_index=True)
    df_fact.to_csv(os.path.join(output_dir, "fact_meteo.csv"), index=False)

    # --- 3) Crée la dimension date ---
    all_dates = pd.to_datetime(df_fact["date"].unique())
    dim_date_rows = []
    for dt in all_dates:
        dim_date_rows.append({
            "date_id": dt.strftime("%Y%m%d"),
            "date": dt.strftime("%Y-%m-%d"),
            "jour": dt.day,
            "mois": dt.month,
            "annee": dt.year,
            "jour_semaine": dt.strftime("%A"),
            "semaine_num": dt.isocalendar()[1]
        })
    df_dim_date = pd.DataFrame(dim_date_rows)
    df_dim_date.to_csv(os.path.join(output_dir, "dim_date.csv"), index=False)

    print(f"[✓] Schéma étoile généré : {output_dir}")
    return output_dir
