from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "dashboard_hourly",
    start_date=datetime(2026, 10, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["reporting"],
) as dag:
    # Optionally chain: daily_price_aggregate >> this, or use ExternalTaskSensor
    BashOperator(
        task_id="build_dashboard",
        bash_command="python /opt/airflow/dags/build_dashboard.py",
        env={"DASHBOARD_DIR": "/opt/dashboard", "CASSANDRA_HOSTS": "cassandra"},
        append_env=True,
    )
