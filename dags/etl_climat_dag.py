from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.extract import get_city_coordinates, get_weather_forecast
from scripts.transform import clean_weather_data
from scripts.load import save_to_csv

CITY = "Paris"
API_KEY = "3308c283529aa761fed27655f2ccbea5"

with DAG("etl_climat_dag", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:

    def extract_coords():
        lat, lon = get_city_coordinates(CITY, API_KEY)
        return {"lat": lat, "lon": lon}

    def extract_weather():
        return get_weather_forecast(CITY, API_KEY)

    def transform():
        raw = get_weather_forecast(CITY, API_KEY)
        df = clean_weather_data(raw)
        return df

    def load():
        raw = get_weather_forecast(CITY, API_KEY)
        df = clean_weather_data(raw)
        save_to_csv(df)

    t1 = PythonOperator(task_id="extract_city_coords", python_callable=extract_coords)
    t2 = PythonOperator(task_id="extract_weather_data", python_callable=extract_weather)
    t3 = PythonOperator(task_id="clean_weather_data", python_callable=transform)
    t4 = PythonOperator(task_id="save_to_csv", python_callable=load)

    t1 >> t2 >> t3 >> t4
