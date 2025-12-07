# dags/dag_load_raw_to_redshift.py
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
#from airflow.providers.amazon.aws.hooks.redshift import RedshiftHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

S3_BUCKET = os.getenv('DEST_BUCKET')
ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID')
REDSHIFT_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/redshift-serverless-role"

# Table configurations
TABLE_CONFIGS = {
    "customers": {
        "schema": "raw_schema",
        "table": "customers",
        "s3_prefix": "raw_datas/customers/",
        "primary_key": "customer_id"
    },
    "agents": {
        "schema": "raw_schema",
        "table": "agents",
        "s3_prefix": "raw_datas/agents/",
        "primary_key": "agent_id"
    },
    "webforms": {
        "schema": "raw_schema",
        "table": "webforms",
        "s3_prefix": "raw_datas/webforms/",
        "primary_key": "form_id"
    },
    "social_media": {
        "schema": "raw_schema",
        "table": "social_media",
        "s3_prefix": "raw_datas/socialmedias/",
        "primary_key": "post_id"
    },
    "call_logs": {
        "schema": "raw_schema",
        "table": "call_logs",
        "s3_prefix": "raw_datas/callcenters/",
        "primary_key": "call_id"
    }
}

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": None,  # Set your notify_failure if needed
    "on_success_callback": None,  # Set your notify_success if needed
    "start_date": datetime(2025, 11, 20),
}

with DAG(
    "dag_load_raw_to_redshift",
    default_args=default_args,
    schedule=None,  # Triggered by extract DAG
    catchup=False,
    max_active_runs=1,
    tags=["redshift", "s3", "etl"],
) as dag:

    # 1. Setup task: Create tracking table
    setup_tracking = PythonOperator(
        task_id="setup_tracking_tables",
        python_callable=lambda: __import__('utilities.redshift_utils').redshift_utils.initialize_file_tracking()
    )

    # 2. Sensors to wait for S3 files
    sensors = []
    for table_name, config in TABLE_CONFIGS.items():
        sensor = S3KeySensor(
            task_id=f"wait_for_{table_name}_in_s3",
            bucket_name=S3_BUCKET,
            bucket_key=f"{config['s3_prefix']}*.parquet",
            aws_conn_id="aws_default",
            mode="poke",
            poke_interval=60,
            timeout=60 * 60 * 2,  # 2 hours
            wildcard_match=True,
        )
        sensors.append(sensor)

    # 3. Function to check for new files
    def check_new_files(table_name: str, **context):
        """
        Check if there are new files to process
        """
        from utilities.redshift_utils import (
            get_s3_files_with_metadata,
            get_processed_files
        )
        
        config = TABLE_CONFIGS[table_name]
        s3_prefix = config["s3_prefix"]
        
        # Get current S3 files
        current_files = get_s3_files_with_metadata(S3_BUCKET, s3_prefix)
        if not current_files:
            print(f"No files found for {table_name} in s3://{S3_BUCKET}/{s3_prefix}")
            context['ti'].xcom_push(key=f"{table_name}_has_new_files", value=False)
            return False
        
        # Get processed files
        processed_files = get_processed_files(table_name)
        
        # Check for new or changed files
        new_files = []
        for file_info in current_files:
            s3_url = f"s3://{S3_BUCKET}/{file_info.key}"
            
            if s3_url not in processed_files:
                new_files.append(file_info)
            else:
                # Check if file changed
                stored_info = processed_files[s3_url]
                if (file_info.etag != stored_info['etag'] or 
                    file_info.size != stored_info['size']):
                    new_files.append(file_info)
        
        has_new_files = len(new_files) > 0
        context['ti'].xcom_push(key=f"{table_name}_has_new_files", value=has_new_files)
        context['ti'].xcom_push(key=f"{table_name}_new_file_count", value=len(new_files))
        
        print(f"Table {table_name}: {len(new_files)} new/changed files out of {len(current_files)} total")
        return has_new_files

    # 4. Check tasks for each table
    check_tasks = []
    for table_name in TABLE_CONFIGS.keys():
        check_task = PythonOperator(
            task_id=f"check_new_files_{table_name}",
            python_callable=check_new_files,
            op_kwargs={"table_name": table_name},
            provide_context=True,
        )
        check_tasks.append(check_task)

    # 5. S3ToRedshiftOperator tasks for incremental load
    copy_tasks = []
    for table_name, config in TABLE_CONFIGS.items():
        copy_task = S3ToRedshiftOperator(
            task_id=f"copy_{table_name}_to_redshift",
            schema=config["schema"],
            table=config["table"],
            s3_bucket=S3_BUCKET,
            s3_key=config["s3_prefix"],
            redshift_conn_id="redshift_default",
            aws_conn_id=None,
            copy_options=[
                "FORMAT AS PARQUET",
                "STATUPDATE OFF",
                "COMPUPDATE OFF",
                f"IAM_ROLE '{REDSHIFT_ROLE_ARN}'"
            ],
            method="APPEND",
            # Skip if no new files (from XCom)
            skip_on_empty=True,
        )
        copy_tasks.append(copy_task)

    # 6. Function to update file tracking after successful load
    def update_tracking_after_load(table_name: str, **context):
        """
        Update file tracking after successful COPY
        """
        from utilities.redshift_utils import (
            get_s3_files_with_metadata,
            update_file_tracking
        )
        
        config = TABLE_CONFIGS[table_name]
        s3_prefix = config["s3_prefix"]
        
        # Get all current files
        current_files = get_s3_files_with_metadata(S3_BUCKET, s3_prefix)
        
        # Update tracking for all files
        for file_info in current_files:
            update_file_tracking(S3_BUCKET, table_name, file_info)
        
        print(f"Updated tracking for {len(current_files)} files in {table_name}")
        return True

    # 7. Update tracking tasks
    update_tasks = []
    for table_name in TABLE_CONFIGS.keys():
        update_task = PythonOperator(
            task_id=f"update_tracking_{table_name}",
            python_callable=update_tracking_after_load,
            op_kwargs={"table_name": table_name},
            provide_context=True,
            trigger_rule="all_done",  # Run even if upstream failed
        )
        update_tasks.append(update_task)

    # 8. Trigger dbt DAG
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="dag_dbt_transform",
        wait_for_completion=False
    )

    # 9. Set up dependencies
    setup_tracking >> sensors >> check_tasks
    
    # Link check tasks to copy tasks (only if there are new files)
    for i, (check_task, copy_task) in enumerate(zip(check_tasks, copy_tasks)):
        table_name = list(TABLE_CONFIGS.keys())[i]
        check_task >> copy_task
        
    # Link copy tasks to update tasks
    for copy_task, update_task in zip(copy_tasks, update_tasks):
        copy_task >> update_task
    
    # All update tasks must complete before triggering dbt
    update_tasks >> trigger_dbt