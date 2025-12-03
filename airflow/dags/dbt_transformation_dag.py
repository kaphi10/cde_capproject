# dags/dag_dbt_transform.py
from airflow import DAG
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from helpers.notify import notify_failure, notify_success
import os

DBT_PROJECT_DIR = "/opt/airflow/dbt"  # adapt to where you store the dbt project in Airflow image
DBT_PROFILES_DIR = "/opt/airflow/.dbt"  # where your profiles.yml is during runtime

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "on_success_callback": notify_success
}

with DAG(
    "dag_dbt_transform",
    start_date=datetime(2025, 11, 20),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # Run staging models first
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps && DBT_PROFILES_DIR={DBT_PROFILES_DIR} dbt run --models staging+",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR}
    )

    # Then marts (dim + fact)
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && DBT_PROFILES_DIR={DBT_PROFILES_DIR} dbt run --models marts+",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR}
    )

    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=f"cd {DBT_PROJECT_DIR} && DBT_PROFILES_DIR={DBT_PROFILES_DIR} dbt test",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR}
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && DBT_PROFILES_DIR={DBT_PROFILES_DIR} dbt docs generate",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR}
    )

    dbt_staging >> dbt_marts >> dbt_tests >> dbt_docs
