from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from dq_checks import TABLES, table_checks

with DAG(
    "nightly_lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    batch = BashOperator(
        task_id="run_lakehouse_batch_job",
        bash_command="docker start -a realestate-lakehouse-batch-job-1",
    )

    # Check the source tables *before* the batch job copies them into the lakehouse,
    # so bad data fails the run instead of landing in bronze/silver/gold.
    for table in TABLES:
        _, last = table_checks(table)
        last >> batch
