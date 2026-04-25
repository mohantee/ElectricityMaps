"""Airflow DAG for Gold Layer — Business aggregations.

Runs daily to aggregate Silver tables into daily reporting tables.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from electricity_maps.layers.gold import transform_gold

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "electricity_maps_gold",
    default_args=default_args,
    description="Aggregate Silver data to Gold layer daily tables",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=["electricity_maps", "gold"],
) as dag:

    def _run_gold(**kwargs):
        result = transform_gold()
        if result.get("status") == "no_pending":
            raise AirflowSkipException("No pending silver batches to process.")
        print(f"Gold aggregation complete: {result}")

    aggregate_task = PythonOperator(
        task_id="transform_gold",
        python_callable=_run_gold,
    )
