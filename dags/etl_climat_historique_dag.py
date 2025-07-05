"""
===============================================================
 DAG : etl_climat_historique_dag
---------------------------------------------------------------
Ce DAG exécute une seule fois un pipeline ETL pour chaque ville
définie dans CITIES :

  1 extract : récupère les données météo historiques 
    depuis Meteostat pour la période START_DATE à END_DATE.
  2 transform : nettoie et transforme les données récupérées.
  3 load : sauvegarde les données nettoyées au format CSV.

Ce pipeline traite uniquement des données historiques.

Les tâches sont chaînées pour chaque ville et les villes
sont traitées les unes après les autres.
===============================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.dirname(__file__)) 

from scripts.extract import get_historical_weather_meteostat
from scripts.transform import clean_weather_data_meteostat
from scripts.load import save_to_csv
from etl_climat_master_dag import CITIES_master

CITIES = CITIES_master
START_DATE = datetime(2025, 4, 25)
END_DATE = datetime.now()

with DAG("etl_climat_historique_dag", 
        start_date=datetime(2024, 1, 1), 
        schedule="@once", 
        catchup=False
    ) as dag:
    
    previous_last_task = None

    for CITY in CITIES:
        
        def extract(CITY):
            def task():
                return get_historical_weather_meteostat(CITY, START_DATE, END_DATE)
            return task

        def transform(CITY):
            def task():
                df = get_historical_weather_meteostat(CITY, START_DATE, END_DATE)
                return clean_weather_data_meteostat(df)
            return task

        def load(CITY):
            def task():
                df = get_historical_weather_meteostat(CITY, START_DATE, END_DATE)
                df_clean = clean_weather_data_meteostat(df)
                save_to_csv(df_clean, f"airflow/dags/climat_tourisme/data/weather_historical_{CITY}.csv")
            return task

        t1 = PythonOperator(task_id=f"extract_historical_weather_{CITY.lower()}", python_callable=extract(CITY))
        t2 = PythonOperator(task_id=f"transform_historical_weather_{CITY.lower()}", python_callable=transform(CITY))
        t3 = PythonOperator(task_id=f"load_historical_weather_{CITY.lower()}", python_callable=load(CITY))
        t1 >> t2 >> t3 

        
        if previous_last_task:
            previous_last_task >> t1  

            previous_last_task = t3 