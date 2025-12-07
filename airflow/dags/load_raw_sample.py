# dags/dag_load_raw_to_redshift_simple.py
from airflow import DAG
from dotenv import load_dotenv
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os

load_dotenv()

S3_BUCKET = os.getenv('DEST_BUCKET')
ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID')
REDSHIFT_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/redshift-serverless-role"

# Table configurations
TABLES = [
    {
        "name": "customers",
        "s3_prefix": "raw_datas/customers/",
    },
    {
        "name": "agents",
        "s3_prefix": "raw_datas/agents/",
    },
    {
        "name": "webforms", 
        "s3_prefix": "raw_datas/webforms/",
    },
    {
        "name": "social_media",
        "s3_prefix": "raw_datas/socialmedias/",
    },
    {
        "name": "call_logs",
        "s3_prefix": "raw_datas/callcenters/",
    }
]

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 11, 20),
}

with DAG(
    "dag_load_raw_to_redshift_simple",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # Wait for all S3 files
    all_sensors = []
    for table in TABLES:
        sensor = S3KeySensor(
            task_id=f"wait_for_{table['name']}",
            bucket_name=S3_BUCKET,
            bucket_key=f"{table['s3_prefix']}*.parquet",
            aws_conn_id="aws_default",
            poke_interval=60,
            timeout=60 * 60 * 2,
            wildcard_match=True,
        )
        all_sensors.append(sensor)

    # Copy each table
    copy_tasks = []
    for table in TABLES:
        copy_task = S3ToRedshiftOperator(
            task_id=f"copy_{table['name']}",
            schema="raw_schema",
            table=table["name"],
            s3_bucket=S3_BUCKET,
            s3_key=table["s3_prefix"],
            redshift_conn_id="redshift_default",
            aws_conn_id=None,
            #aws_conn_id="aws_default",
            copy_options=[
                "FORMAT AS PARQUET",
                f"IAM_ROLE '{REDSHIFT_ROLE_ARN}'",
                "STATUPDATE OFF",
                "COMPUPDATE OFF",
            ],
            method="APPEND",
        )
        copy_tasks.append(copy_task)

    # Trigger next DAG
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="dag_dbt_transform",
        wait_for_completion=False
    )

    # Set dependencies
    #all_sensors >> copy_tasks >> trigger_dbt
    from airflow.models.baseoperator import chain
    # Method 1: Using chain
    chain(*all_sensors, *copy_tasks, trigger_dbt)
