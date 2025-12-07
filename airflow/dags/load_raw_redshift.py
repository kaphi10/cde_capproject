# dags/dag_load_raw_to_redshift.py
from airflow import DAG
from pendulum import datetime
from dotenv import load_dotenv
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from utilities.helper import list_keys_with_prefix
from airflow.dags.utilities.redshift_utils_t import (
    get_redshift_conn_info, 
    run_sql_script, 
    table_has_recent_load,
    initialize_file_tracking,
    get_unprocessed_files,
    create_manifest_file,
    update_file_tracking,
    cleanup_old_manifests,
    get_s3_files_with_metadata,
    verify_s3_urls
)
from utilities.notify import notify_failure, notify_success

import os
load_dotenv()

S3_BUCKET = os.getenv('DEST_BUCKET')
ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID')
REDSHIFT_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/redshift-serverless-role"

# Table configurations
TABLE_CONFIGS = {
    "customers": {
        "schema": "raw_schema",
        "s3_prefix": "raw_datas/customers/",
        "primary_key": "customer_id",  # Optional, for MERGE operations
        "strategy": "incremental"  # incremental or full
    },
    "agents": {
        "schema": "raw_schema",
        "s3_prefix": "raw_datass/agents/",
        "primary_key": "agent_id",
        "strategy": "incremental"
    },
    "webforms": {
        "schema": "raw_schema",
        "s3_prefix": "raw_datas/webforms/",
        "primary_key": "request_id",
        "strategy": "incremental"
    },
    "social_media": {
        "schema": "raw_schema",
        "s3_prefix": "raw_datas/socialmedia/",
        "primary_key": "complaint_id",
        "strategy": "incremental"
    },
    "call_logs": {
        "schema": "raw_schema",
        "s3_prefix": "raw_datas/callcenter/",
        "primary_key": "call_id",
        "strategy": "incremental"
    }
}

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "on_success_callback": notify_success
}

def check_s3_files(prefix: str, **context):
    """
    Check if there are any files in the S3 prefix
    """
    keys = list_keys_with_prefix(S3_BUCKET, prefix)
    if not keys:
        raise ValueError(f"No files found in s3://{S3_BUCKET}/{prefix}")
    
    # Store number of files found in XCom
    context["ti"].xcom_push(key=f"files_{prefix}", value=len(keys))
    return True

def setup_tracking_tables(**context):
    """
    Initialize file tracking table
    """
    print("Initializing file tracking table...")
    initialize_file_tracking()
    print("File tracking table initialized successfully")
    return True


# def run_incremental_copy(table_name: str, **context):
#     """
#     Run incremental COPY for a specific table
#     """
#     config = TABLE_CONFIGS.get(table_name)
#     if not config:
#         raise ValueError(f"No configuration found for table: {table_name}")
    
#     schema = config["schema"]
#     s3_prefix = config["s3_prefix"]
    
#     print(f"\n{'='*60}")
#     print(f"Processing {table_name}")
#     print(f"{'='*60}")
    
#     # Step 1: Find unprocessed files
#     unprocessed_files, file_urls = get_unprocessed_files(
#         bucket=S3_BUCKET,
#         table_name=table_name,
#         s3_prefix=s3_prefix
#     )
    
#     if not unprocessed_files:
#         print(f"✓ No new/changed files for {table_name}")
#         context["ti"].xcom_push(key=f"{table_name}_processed", value=0)
#         return {"status": "skipped", "files": 0}
    
#     print(f"Found {len(unprocessed_files)} new/changed files")
    
#     # Step 2: Create manifest file for all new files
#     manifest_url = create_manifest_file(S3_BUCKET, file_urls)
#     if not manifest_url:
#         print(f"✗ Failed to create manifest for {table_name}")
#         return {"status": "failed", "files": 0}
    
#     # Step 3: Execute COPY using manifest
#     copy_sql = f"""
#         COPY {schema}.{table_name}
#         FROM '{manifest_url}'
#         IAM_ROLE '{REDSHIFT_ROLE_ARN}'
#         MANIFEST
#         FORMAT AS PARQUET
#         STATUPDATE OFF
#         COMPUPDATE OFF
#     """
    
#     try:
#         print(f"Executing COPY for {table_name}...")
#         run_sql_script(copy_sql)
        
#         # Step 4: Get approximate rows loaded (you might need to adjust this)
#         # One approach is to count rows after load
#         count_sql = f"""
#             SELECT COUNT(*) 
#             FROM {schema}.{table_name} 
#             WHERE load_timestamp >= SYSDATE - INTERVAL '10 minutes'
#         """
#         result = run_sql_script(count_sql, fetch_results=True)
#         rows_loaded = result[0][0] if result else 0
        
#         # Step 5: Update file tracking
#         rows_per_file = rows_loaded // len(unprocessed_files) if unprocessed_files else 0
#         # for file_info in unprocessed_files:
#         #     update_file_tracking(table_name, file_info, rows_per_file)
#         for file_info in unprocessed_files:
#             update_file_tracking(S3_BUCKET, table_name, file_info, rows_per_file)
        
#         print(f"✓ Successfully loaded {rows_loaded} rows into {table_name}")
        
#         context["ti"].xcom_push(key=f"{table_name}_processed", value=len(unprocessed_files))
#         context["ti"].xcom_push(key=f"{table_name}_rows", value=rows_loaded)
        
#         return {
#             "status": "success",
#             "files": len(unprocessed_files),
#             "rows": rows_loaded
#         }
        
#     except Exception as e:
#         print(f"✗ Error loading {table_name}: {str(e)}")
#         raise

def run_incremental_copy(table_name: str, **context):
    """
    Run incremental COPY for a specific table
    """
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        raise ValueError(f"No configuration found for table: {table_name}")
    
    schema = config["schema"]
    s3_prefix = config["s3_prefix"]
    
    print(f"\n{'='*60}")
    print(f"Processing {table_name}")
    print(f"{'='*60}")
    
    # Step 1: Find unprocessed files
    unprocessed_files, file_urls = get_unprocessed_files(
        bucket=S3_BUCKET,
        table_name=table_name,
        s3_prefix=s3_prefix
    )
    
    if not unprocessed_files:
        print(f"✓ No new/changed files for {table_name}")
        context["ti"].xcom_push(key=f"{table_name}_processed", value=0)
        return {"status": "skipped", "files": 0}
    
    print(f"Found {len(unprocessed_files)} new/changed files")
    
    # Step 1.5: VERIFY file URLs are accessible
    print("Verifying S3 file accessibility...")
    valid_urls = verify_s3_urls(S3_BUCKET, file_urls)
    
    if not valid_urls:
        print("✗ No accessible files found")
        return {"status": "failed", "files": 0}
    
    if len(valid_urls) != len(file_urls):
        print(f"WARNING: Only {len(valid_urls)}/{len(file_urls)} files are accessible")
    
    # Step 2: Create manifest file for VALID files only
    manifest_url = create_manifest_file(S3_BUCKET, valid_urls)
    if not manifest_url:
        print(f"✗ Failed to create manifest for {table_name}")
        return {"status": "failed", "files": 0}
    
    # DEBUG: Print the COPY SQL before executing
    copy_sql = f"""
        COPY {schema}.{table_name}
        FROM '{manifest_url}'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        MANIFEST
        FORMAT AS PARQUET
        STATUPDATE OFF
        COMPUPDATE OFF
    """
    
    print(f"DEBUG: COPY SQL to execute:")
    print(copy_sql)
    
    try:
        print(f"Executing COPY for {table_name}...")
        
        # First, test with a simple COPY to verify permissions
        test_sql = f"""
            SELECT COUNT(*) 
            FROM {schema}.{table_name} 
            LIMIT 1
        """
        print(f"Testing table access with: {test_sql}")
        run_sql_script(test_sql, fetch_results=True)
        
        # Now execute the COPY
        run_sql_script(copy_sql)
        
        # Step 4: Get approximate rows loaded (you might need to adjust this)
        # One approach is to count rows after load
        count_sql = f"""
            SELECT COUNT(*) 
            FROM {schema}.{table_name} 
            WHERE load_timestamp >= SYSDATE - INTERVAL '10 minutes'
        """
        result = run_sql_script(count_sql, fetch_results=True)
        rows_loaded = result[0][0] if result else 0
        
        # Step 5: Update file tracking
        rows_per_file = rows_loaded // len(unprocessed_files) if unprocessed_files else 0
        # for file_info in unprocessed_files:
        #     update_file_tracking(table_name, file_info, rows_per_file)
        for file_info in unprocessed_files:
            update_file_tracking(S3_BUCKET, table_name, file_info, rows_per_file)
        
        print(f"✓ Successfully loaded {rows_loaded} rows into {table_name}")
        
        context["ti"].xcom_push(key=f"{table_name}_processed", value=len(unprocessed_files))
        context["ti"].xcom_push(key=f"{table_name}_rows", value=rows_loaded)
        
        return {
            "status": "success",
            "files": len(unprocessed_files),
            "rows": rows_loaded
        }
        
    except Exception as e:
        print(f"✗ Error loading {table_name}: {str(e)}")
        raise
        
        # ... rest of your code ...

def run_copy_all(**context):
    """
    Run COPY for all tables (maintains backward compatibility)
    """
    # For backward compatibility, we can still use the old method
    # or switch to incremental method
    results = {}
    
    for table_name in TABLE_CONFIGS.keys():
        result = run_incremental_copy(table_name, **context)
        results[table_name] = result
    
    # Print summary
    print("\n" + "="*60)
    print("LOAD SUMMARY")
    print("="*60)
    
    total_files = 0
    total_rows = 0
    
    for table, result in results.items():
        status = result.get('status', 'unknown')
        files = result.get('files', 0)
        rows = result.get('rows', 0)
        
        print(f"{table:20} | {status:10} | Files: {files:4} | Rows: {rows:8}")
        
        total_files += files
        total_rows += rows
    
    print(f"\nTotal: {total_files} files, {total_rows} rows processed")
    
    # Clean up old manifests
    try:
        cleanup_old_manifests(S3_BUCKET)
    except Exception as e:
        print(f"Note: Failed to clean up old manifests: {e}")
    
    return results

def check_all_s3_files(**context):
    """
    Check all required S3 prefixes have files
    """
    prefixes_to_check = [
        "raw_datas/customers/",
        "raw_datas/callcenters/",
        "raw_datas/webforms/",
        "raw_datas/socialmedias/",
        "raw_datas/agents/"
    ]
    
    all_ready = True
    for prefix in prefixes_to_check:
        try:
            keys = list_keys_with_prefix(S3_BUCKET, prefix)
            if not keys:
                print(f"Warning: No files found in {prefix}")
                all_ready = False
            else:
                print(f"Found {len(keys)} files in {prefix}")
        except Exception as e:
            print(f"Error checking {prefix}: {e}")
            all_ready = False
    
    if not all_ready:
        raise ValueError("Some S3 prefixes are empty or inaccessible")
    
    return True

with DAG(
    "dag_load_raw_to_redshift",
    start_date=datetime(2025, 11, 20),
    schedule=None,  # triggered by extract DAG
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # Sensor to wait for all S3 files
    check_s3_all = PythonSensor(
        task_id="wait_for_all_s3_files",
        python_callable=check_all_s3_files,
        poke_interval=60,
        timeout=60 * 60 * 2,  # 2 hours
        mode="poke",
        do_xcom_push=True
    )

    # Initialize tracking tables
    setup_tables = PythonOperator(
        task_id="setup_tracking_tables",
        python_callable=setup_tracking_tables
    )

    # Main copy task (handles all tables)
    t_copy = PythonOperator(
        task_id="copy_raw_to_redshift",
        python_callable=run_copy_all,
       # provide_context=True
    )

    # Trigger dbt DAG after successful load
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="dag_dbt_transform",
        wait_for_completion=False
    )

    # Parallel table loads (optional - for faster processing)
    # Uncomment if you want to load tables in parallel
    """
    copy_tasks = []
    for table_name in TABLE_CONFIGS.keys():
        task = PythonOperator(
            task_id=f"copy_{table_name}",
            python_callable=run_incremental_copy,
            op_kwargs={'table_name': table_name},
            provide_context=True
        )
        copy_tasks.append(task)
    
    # Update dependencies
    check_s3_all >> setup_tables >> copy_tasks >> trigger_dbt
    """
    
    # Original sequential flow
    check_s3_all >> setup_tables >> t_copy >> trigger_dbt