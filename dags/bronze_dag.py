"""Airflow DAG for Bronze Layer — Raw data ingestion.

Runs hourly to ingest electricity mix and flows from the API to S3.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from electricity_maps.layers.bronze import ingest_bronze

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "electricity_maps_bronze",
    default_args=default_args,
    description="Ingest raw data from Electricity Maps API to Bronze layer",
    schedule_interval="@hourly",
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=["electricity_maps", "bronze"],
) as dag:

    def _run_bronze(**kwargs):
        result = ingest_bronze()
        print(f"Bronze ingestion complete: {result}")

    ingest_task = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_run_bronze,
    )
