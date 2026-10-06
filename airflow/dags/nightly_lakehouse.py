from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    "nightly_lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    BashOperator(
        task_id="run_lakehouse_batch_job",
        bash_command="docker start -a realestate-lakehouse-batch-job-1",
    )