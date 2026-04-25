"""Airflow DAG for Silver Layer — Data transformation and cleansing.

Runs hourly (offset by 10 minutes) to process ready Bronze data.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from electricity_maps.layers.silver import transform_silver

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "electricity_maps_silver",
    default_args=default_args,
    description="Transform Bronze data to Silver layer Delta tables",
    schedule_interval="10 * * * *",  # 10 minutes past the hour
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=["electricity_maps", "silver"],
) as dag:

    def _run_silver(**kwargs):
        result = transform_silver()
        if result.get("status") == "no_pending":
            raise AirflowSkipException("No pending bronze batches to process.")
        print(f"Silver transformation complete: {result}")

    transform_task = PythonOperator(
        task_id="transform_silver",
        python_callable=_run_silver,
    )
