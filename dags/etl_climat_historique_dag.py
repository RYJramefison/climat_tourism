from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.extract import get_historical_weather_meteostat
from scripts.transform import clean_weather_data_meteostat
from scripts.load import save_to_csv

CITY = "Antananarivo"
START_DATE = datetime(2024, 11, 1)
END_DATE = datetime(2024, 12, 31)

with DAG("etl_climat_historique_dag", start_date=datetime(2024, 1, 1), schedule="@once", catchup=False) as dag:

    def extract():
        return get_historical_weather_meteostat(CITY, START_DATE, END_DATE)

    def transform():
        df = get_historical_weather_meteostat(CITY, START_DATE, END_DATE)
        return clean_weather_data_meteostat(df)

    def load():
        df = get_historical_weather_meteostat(CITY, START_DATE, END_DATE)
        df_clean = clean_weather_data_meteostat(df)
        save_to_csv(df_clean, f"airflow/dags/climat_tourisme/data/weather_historical_{CITY}.csv")

    t1 = PythonOperator(task_id="extract_historical_weather", python_callable=extract)
    t2 = PythonOperator(task_id="transform_historical_weather", python_callable=transform)
    t3 = PythonOperator(task_id="load_historical_weather", python_callable=load)

    t1 >> t2 >> t3
