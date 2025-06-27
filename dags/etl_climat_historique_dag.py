from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

current_dir = os.path.dirname(__file__)
sys.path.append(current_dir) 

from scripts.extract import get_historical_weather_meteostat
from scripts.transform import clean_weather_data_meteostat
from scripts.load import save_to_csv
from scripts.aggregate import aggregate_scores_by_month
from etl_climat_master_dag import CITIES_master

CITIES = CITIES_master
START_DATE = datetime(2025, 3, 1)
END_DATE = datetime(2025, 4, 1)

with DAG("etl_climat_historique_dag", start_date=datetime(2024, 1, 1), schedule="@once", catchup=False) as dag:
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
        
        def aggregate(CITY):
            def task():
                aggregate_scores_by_month([CITY])
            return task

        t1 = PythonOperator(task_id=f"extract_historical_weather_{CITY.lower()}", python_callable=extract(CITY))
        t2 = PythonOperator(task_id=f"transform_historical_weather_{CITY.lower()}", python_callable=transform(CITY))
        t3 = PythonOperator(task_id=f"load_historical_weather_{CITY.lower()}", python_callable=load(CITY))
        t4 = PythonOperator(task_id=f"aggregate_monthly_score_{CITY.lower()}", python_callable=aggregate(CITY))
        t1 >> t2 >> t3 >> t4


        if previous_last_task:
            previous_last_task >> t1  # la dernière tâche précédente vers le t1 de cette ville

            previous_last_task = t4 