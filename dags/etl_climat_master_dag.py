from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.combine import combine_historical_and_recent
from scripts.transform import transform_to_star

CITIES_master = ["Antananarivo", "Paris"]

DATA_DIR = "airflow/dags/climat_tourisme/data"  # ✅ corrigé ici

def make_combine_task(city):
    def task():
        combine_historical_and_recent(city, data_dir=DATA_DIR)
    return task

with DAG(
        dag_id="etl_climat_master_dag",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,  # ✅ Ne permet qu'un seul run actif à la fois
        # concurrency=2       # ✅ Au maximum 2 tâches exécutées en parallèle
    ) as dag:

    # ✅ Ces DAG doivent déjà exister et être activés dans Airflow
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
    # ✅ Chaque ville sera fusionnée après l'exécution des deux DAGs
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
