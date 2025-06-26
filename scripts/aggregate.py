# scripts/aggregate.py

import pandas as pd
import os

def aggregate_scores_by_month(cities, input_dir="airflow/dags/climat_tourisme/data", output_dir=None):
    """
    Agrège les scores météo journaliers en moyenne mensuelle pour chaque ville.
    
    Args:
        cities (list): Liste des noms de villes.
        input_dir (str): Dossier contenant les fichiers weather_<ville>.csv.
        output_dir (str): Dossier pour sauvegarder les fichiers agrégés. Par défaut = input_dir.
    """
    if output_dir is None:
        output_dir = input_dir

    for city in cities:
        file_path = os.path.join(input_dir, f"weather_historical_{city}.csv")
        if not os.path.exists(file_path):
            print(f"[!] Fichier introuvable : {file_path}")
            continue

        try:
            df = pd.read_csv(file_path)
            df["date"] = pd.to_datetime(df["date"])
            df["month"] = df["date"].dt.to_period("M")
            
            df_monthly = df.groupby("month")["score"].mean().reset_index()
            df_monthly["score"] = df_monthly["score"].round(1)
            df_monthly["city"] = city

            output_path = os.path.join(output_dir, f"score_monthly_{city}.csv")
            df_monthly.to_csv(output_path, index=False)
            print(f"[✓] Sauvegardé : {output_path}")
        
        except Exception as e:
            print(f"[X] Erreur avec {city}: {e}")
