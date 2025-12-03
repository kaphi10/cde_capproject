# dags/dag_extract_raw.py
from airflow import DAG
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from utilities.notify import notify_failure, notify_success
from utilities.helper import ensure_local_tmp

from collect_data.get_data import extract_customers, extract_agents, extract_callcenter, extract_social_media, extract_webforms


default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "on_success_callback": notify_success
}

with DAG(
    "dag_extract_raw",
    start_date=datetime(2025, 11, 20),
    schedule=None,
    #schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # Ensure tmp exists (useful for local / container)
    t0 = PythonOperator(
        task_id="ensure_tmp_dir",
        python_callable=lambda: ensure_local_tmp("tmp")
    )

    t_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=lambda: extract_customers(
            source_bucket="your-source-bucket",  # replace or make dynamic
            dest_bucket="core-telecoms-data-lake",
            session=None  # if your extract_customers expects session, adapt accordingly
        )
    )

    t_agents = PythonOperator(
        task_id="extract_agents",
        python_callable=lambda: extract_agents(
            source_bucket="your-source-bucket",
            dest_bucket="core-telecoms-data-lake",
            session=None
        )
    )

    t_callcenter = PythonOperator(
        task_id="extract_callcenter",
        python_callable=lambda: extract_callcenter(
            source_bucket="your-source-bucket",
            dest_bucket="core-telecoms-data-lake",
            session=None
        )
    )

    t_social = PythonOperator(
        task_id="extract_socialmedia",
        python_callable=lambda: extract_social_media(
            source_bucket="your-source-bucket",
            dest_bucket="core-telecoms-data-lake",
            session=None
        )
    )

    t_webforms = PythonOperator(
        task_id="extract_webforms",
        python_callable=lambda: extract_webforms(
            source_bucket="your-source-bucket",
            dest_bucket="core-telecoms-data-lake",
            session=None
        )
    )

    # Trigger load DAG when extraction completes
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_to_redshift_dag",
        trigger_dag_id="dag_load_raw_to_redshift",
        wait_for_completion=False
    )

    # ordering
    t0 >> [t_customers, t_agents, t_callcenter, t_social, t_webforms] >> trigger_load
