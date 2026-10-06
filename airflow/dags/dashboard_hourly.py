from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    "dashboard_hourly",
    start_date=datetime(2026, 10, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:
    BashOperator(
        task_id="run_heatmap_job",
        bash_command="docker start -a realestate-heatmap-job-1",
    )
