from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="banvic_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["banvic", "ingestion"],
) as dag:


    ingestao_meltano = BashOperator(
        task_id="ingestao_meltano",
        bash_command="meltano --environment=k8s run tap-csv target-postgres",
        cwd=PROJECT_DIR,
        append_env=True,
    )