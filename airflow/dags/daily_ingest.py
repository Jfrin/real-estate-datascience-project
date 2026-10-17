from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from dq_checks import TABLES, table_checks

with DAG("daily_ingest", start_date=datetime(2026, 1, 1), schedule_interval="@hourly", catchup=False) as dag:
    pollers = [
        BashOperator(
            task_id=f"run_{name.replace('-', '_')}",
            bash_command=f"docker start -a realestate-{name}-1",
        )
        for name in ["dob-permits-poller", "acris-poller", "pluto-poller"]
    ]

    with TaskGroup("data_quality"):
        for table in TABLES:
            first, _ = table_checks(table)
            pollers >> first
