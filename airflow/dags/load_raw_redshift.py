# dags/dag_load_raw_to_redshift.py
from airflow import DAG
from pendulum import datetime
from dotenv import load_dotenv
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from utilities.helper import list_keys_with_prefix
from utilities.redshift_utils import get_redshift_conn_info,run_sql_script, table_has_recent_load
from utilities.notify import notify_failure, notify_success

import os
load_dotenv()
S3_BUCKET = os.getenv('DEST_BUCKET') # replace if different
ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID')  # replace with your AWS account ID
REDSHIFT_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/redshift-serverless-role"  # REPLACE

COPY_STATEMENTS = {
    "raw.customers": f"""
        COPY raw_schema.customers FROM 's3://{S3_BUCKET}/raw/customers/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;
    """,
    "raw.call_logs": f"""
        COPY raw_schema.call_logs
        FROM 's3://{S3_BUCKET}/raw/callcenter/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;
    """,
    "raw.social_media": f"""
        COPY raw_schema.social_media
        FROM 's3://{S3_BUCKET}/raw/socialmedia/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;
    """,
    "raw.webforms": f"""
        COPY raw_schema.webforms
        FROM 's3://{S3_BUCKET}/raw/webforms/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;
    """,
    "raw.agents": f"""
        COPY raw_schema.agents
        FROM 's3://{S3_BUCKET}/raw/agents/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;
    """,
}

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "on_success_callback": notify_success
}

with DAG(
    "dag_load_raw_to_redshift",
    start_date=datetime(2025, 11, 20),
    schedule=None,  # triggered by extract DAG
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    def check_s3_files(prefix: str, **context):
        keys = list_keys_with_prefix(S3_BUCKET, prefix)
        if not keys:
            raise ValueError(f"No files found in s3://{S3_BUCKET}/{prefix}")
        # store number of files found in XCom
        context["ti"].xcom_push(key=f"files_{prefix}", value=len(keys))
        return True

    def run_copy_all(**context):
        # For idempotency: if raw table already has rows in last 24 hours, skip COPY for that table.
        for table, copy_sql in COPY_STATEMENTS.items():
            # idempotency cheumck
            try:
                already = table_has_recent_load(table, cutoff_ts="1 day")
            except Exception:
                already = False

            if already:
                # skip table
                print(f"[SKIP] {table} already has recent load; skipping COPY.")
                continue

            print(f"[COPY] Running COPY for {table}")
            run_sql_script(copy_sql)
        return True

    check_customers = PythonSensor(
        task_id="wait_for_customers_in_s3",
        python_callable=lambda: bool(list_keys_with_prefix(S3_BUCKET, "raw/customers/")),
        poke_interval=60,
        timeout=60 * 60 * 2,  # 2 hours
        mode="poke"
    )

    check_callcenter = PythonSensor(
        task_id="wait_for_callcenter_in_s3",
        python_callable=lambda: bool(list_keys_with_prefix(S3_BUCKET, "raw/callcenter/")),
        poke_interval=60,
        timeout=60 * 60 * 2,
        mode="poke"
    )

    # We can run a single copy task that handles all tables
    t_copy = PythonOperator(
        task_id="copy_raw_to_redshift",
        python_callable=run_copy_all,
        #provide_context=True
    )

    # Trigger dbt DAG after successful load
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="dag_dbt_transform",
        wait_for_completion=False
    )

    [check_customers, check_callcenter] >> t_copy >> trigger_dbt
