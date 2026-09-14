from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("nightly_lakehouse", start_date=datetime(2026, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    BashOperator(
        task_id="run_lakehouse_batch_job",
        bash_command="docker start -a realestate-lakehouse-batch-job-1",
    )
