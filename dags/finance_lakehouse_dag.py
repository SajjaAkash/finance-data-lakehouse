from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from finance_lakehouse.jobs.glue_market_bronze import render_manifest as render_market_manifest
from finance_lakehouse.jobs.glue_sec_bronze import render_manifest as render_sec_manifest


def log_market_manifest() -> None:
    print(render_market_manifest(["AAPL", "MSFT", "NVDA"]))


def log_sec_manifest() -> None:
    print(render_sec_manifest(["320193", "789019", "1045810"], ["AAPL", "MSFT", "NVDA"]))


with DAG(
    dag_id="finance_lakehouse_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["finance", "lakehouse", "mwaa"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_market_data = PythonOperator(
        task_id="ingest_market_data",
        python_callable=log_market_manifest,
    )

    ingest_sec_submissions = PythonOperator(
        task_id="ingest_sec_submissions",
        python_callable=log_sec_manifest,
    )

    run_silver_processing = EmptyOperator(task_id="run_silver_processing")
    run_dbt_gold_models = EmptyOperator(task_id="run_dbt_gold_models")
    end = EmptyOperator(task_id="end")

    (
        start
        >> [ingest_market_data, ingest_sec_submissions]
        >> run_silver_processing
        >> run_dbt_gold_models
        >> end
    )
