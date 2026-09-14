from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("daily_ingest", start_date=datetime(2026, 1, 1), schedule_interval="@hourly", catchup=False) as dag:
    for name in ["dob-permits-poller", "acris-poller", "pluto-poller"]:
        BashOperator(
            task_id=f"run_{name.replace('-', '_')}",
            bash_command=f"docker start -a realestate-{name}-1",
        )
