"""
===============================================================
 DAG : etl_climat_master_dag
---------------------------------------------------------------
Ce DAG joue le rôle de pipeline maître :
il orchestre et déclenche deux sous-DAGs :
  - etl_climat_dag (météo actuelle + prévisions)
  - etl_climat_historique_dag (météo historique)

Ensuite, pour chaque ville de CITIES_master :
  1 combine : fusionne les données historiques et récentes.
  2 generate_star_schema : génère le schéma en étoile 
      pour préparer l'analyse (Data Warehouse).

Le DAG attend que les sous-DAGs soient terminés avant de lancer
la combinaison puis la transformation finale.

Planification : exécution quotidienne.
===============================================================
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.combine import combine_historical_and_recent
from scripts.transform import transform_to_star

CITIES_master = ["Antananarivo", "Paris", "Rome"]

DATA_DIR = "airflow/dags/climat_tourisme/data"

def make_combine_task(city):
    def task():
        combine_historical_and_recent(city, data_dir=DATA_DIR)
    return task

with DAG(
        dag_id="etl_climat_master_dag",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,  
    ) as dag:

    trigger_recent = TriggerDagRunOperator(
        task_id="trigger_recent_dag",
        trigger_dag_id="etl_climat_dag",
        wait_for_completion=True
    )

    trigger_historique = TriggerDagRunOperator(
        task_id="trigger_historique_dag",
        trigger_dag_id="etl_climat_historique_dag",
        wait_for_completion=True
    )

    combine_tasks = []
    for CITY in CITIES_master:
        combine_task = PythonOperator(
            task_id=f"combine_weather_{CITY.lower()}",
            python_callable=make_combine_task(CITY)
        )
        [trigger_recent, trigger_historique] >> combine_task
        combine_tasks.append(combine_task)
        
    generate_star_task = PythonOperator(
        task_id="generate_star_schema",
        python_callable=transform_to_star
    )

    combine_tasks >> generate_star_task
