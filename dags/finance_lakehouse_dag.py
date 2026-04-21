from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from finance_lakehouse.config import settings
from finance_lakehouse.jobs.glue_market_bronze import run_job as market_bronze_preview
from finance_lakehouse.jobs.glue_sec_bronze import run_job as sec_bronze_preview

AWS_CONN_ID = "aws_default"
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/finance_lakehouse"
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/finance_lakehouse"
MARKET_BRONZE_JOB_EXPR = (
    "{{ var.value.finance_market_bronze_job | default('finance-market-bronze') }}"
)
SEC_BRONZE_JOB_EXPR = "{{ var.value.finance_sec_bronze_job | default('finance-sec-bronze') }}"
MARKET_SILVER_JOB_EXPR = (
    "{{ var.value.finance_market_silver_job | default('finance-market-silver') }}"
)
SEC_SILVER_JOB_EXPR = "{{ var.value.finance_sec_silver_job | default('finance-sec-silver') }}"


def preview_market_job() -> None:
    print(market_bronze_preview([]))


def preview_sec_job() -> None:
    print(sec_bronze_preview([]))


with DAG(
    dag_id="finance_lakehouse_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
    },
    tags=["finance", "lakehouse", "mwaa", "dbt", "glue"],
    description="Bronze to Gold finance lakehouse pipeline orchestrated in MWAA.",
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="bronze_ingestion") as bronze_ingestion:
        preview_market = PythonOperator(
            task_id="preview_market_glue_payload",
            python_callable=preview_market_job,
        )
        preview_sec = PythonOperator(
            task_id="preview_sec_glue_payload",
            python_callable=preview_sec_job,
        )

        submit_market_glue_job = BashOperator(
            task_id="submit_market_glue_job",
            bash_command=(
                "echo aws glue start-job-run "
                f"--job-name {MARKET_BRONZE_JOB_EXPR} "
                "--arguments '--execution_date={{ ds }}T00:00:00Z,--raw_bucket="
                + settings.aws.raw_bucket
                + "'"
            ),
        )

        submit_sec_glue_job = BashOperator(
            task_id="submit_sec_glue_job",
            bash_command=(
                "echo aws glue start-job-run "
                f"--job-name {SEC_BRONZE_JOB_EXPR} "
                "--arguments '--execution_date={{ ds }}T00:00:00Z,--raw_bucket="
                + settings.aws.raw_bucket
                + "'"
            ),
        )

        chain(preview_market, submit_market_glue_job)
        chain(preview_sec, submit_sec_glue_job)

    with TaskGroup(group_id="silver_processing") as silver_processing:
        submit_market_silver_glue_job = BashOperator(
            task_id="submit_market_silver_glue_job",
            bash_command=(
                "echo aws glue start-job-run "
                f"--job-name {MARKET_SILVER_JOB_EXPR} "
                "--arguments '--execution_date={{ ds }}T00:00:00Z,--processed_bucket="
                + settings.aws.processed_bucket
                + "'"
            ),
        )

        submit_sec_silver_glue_job = BashOperator(
            task_id="submit_sec_silver_glue_job",
            bash_command=(
                "echo aws glue start-job-run "
                f"--job-name {SEC_SILVER_JOB_EXPR} "
                "--arguments '--execution_date={{ ds }}T00:00:00Z,--processed_bucket="
                + settings.aws.processed_bucket
                + "'"
            ),
        )

    run_dbt_seed = BashOperator(
        task_id="run_dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt seed --profiles-dir {DBT_PROFILES_DIR} --target {settings.platform.dbt_target}"
        ),
    )

    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --profiles-dir {DBT_PROFILES_DIR} --target {settings.platform.dbt_target}"
        ),
    )

    publish_summary = BashOperator(
        task_id="publish_summary",
        bash_command=(
            "echo Pipeline completed for {{ ds }} "
            f"using AWS connection {AWS_CONN_ID} in environment {settings.platform.environment}"
        ),
    )

    end = EmptyOperator(task_id="end")

    chain(
        start,
        bronze_ingestion,
        silver_processing,
        run_dbt_seed,
        run_dbt_build,
        publish_summary,
        end,
    )
