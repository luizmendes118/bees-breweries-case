from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.ingestion.api_client import fetch_breweries, save_bronze
from src.processing.bronze_to_silver import transform_bronze_to_silver
from src.processing.silver_to_gold import transform_silver_to_gold


def ingest():

    data = fetch_breweries()
    save_bronze(data)


default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2026, 3, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}


dag = DAG(
    "breweries_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)


ingestion_task = PythonOperator(
    task_id="ingestion",
    python_callable=ingest,
    dag=dag
)


bronze_silver_task = PythonOperator(
    task_id="bronze_to_silver",
    python_callable=transform_bronze_to_silver,
    dag=dag
)


silver_gold_task = PythonOperator(
    task_id="silver_to_gold",
    python_callable=transform_silver_to_gold,
    dag=dag
)


ingestion_task >> bronze_silver_task >> silver_gold_task
