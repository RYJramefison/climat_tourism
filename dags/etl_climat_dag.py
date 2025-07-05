from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models.variable import Variable
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

current_dir = os.path.dirname(__file__)
sys.path.append(current_dir) 

from scripts.extract import get_city_coordinates, get_weather_forecast
from scripts.transform import clean_weather_data
from scripts.load import save_to_csv
from etl_climat_master_dag import CITIES_master

CITIES = CITIES_master
API_KEY = Variable.get("API_KEY_climat_tourisme")

with DAG("etl_climat_dag",
        start_date=datetime(2024, 1, 1),
        schedule="@daily", 
        catchup=False
    ) as dag:
    
    previous_last_task = None
    
    for CITY in CITIES:
        
        def extract_coords(CITY):
            def task():
                lat, lon = get_city_coordinates(CITY, API_KEY)
                return {"lat": lat, "lon": lon}
            return task

        def extract_weather(CITY):
            def task():
                return get_weather_forecast(CITY, API_KEY)
            return task

        def transform(CITY):
            def task():
                raw = get_weather_forecast(CITY, API_KEY)
                df = clean_weather_data(raw)
                return df
            return task

        def load(CITY):
            def task():
                raw = get_weather_forecast(CITY, API_KEY)
                df = clean_weather_data(raw)
                save_to_csv(df,"airflow/dags/climat_tourisme/data/weather_"+CITY+".csv")
            return task
    
        t1 = PythonOperator(task_id=f"extract_city_{CITY.lower()}", python_callable=extract_coords(CITY))
        t2 = PythonOperator(task_id=f"extract_data_{CITY.lower()}", python_callable=extract_weather(CITY))
        t3 = PythonOperator(task_id=f"clean_data_{CITY.lower()}", python_callable=transform(CITY))
        t4 = PythonOperator(task_id=f"save_to_csv_{CITY.lower()}", python_callable=load(CITY))

        t1 >> t2 >> t3 >> t4
        
        # la dernière tâche précédente vers le t1 de cette ville
        if previous_last_task:
            previous_last_task >> t1  

        previous_last_task = t4 
