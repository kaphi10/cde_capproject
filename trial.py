from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime

with DAG(
    dag_id="s3_to_redshift_with_manifest",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    copy_with_manifest = S3ToRedshiftOperator(
        task_id="copy_with_manifest",
        schema="public",  # Redshift schema
        table="my_table",  # Redshift table
        s3_bucket="my-bucket-name",
        s3_key="path/to/manifest.json",  # Manifest file path in S3
        copy_options=[
            "FORMAT AS JSON 'auto'",
            "MANIFEST",  # Important for manifest usage
            "TIMEFORMAT 'auto'",
            "COMPUPDATE OFF",
            "STATUPDATE OFF"
        ],
        aws_conn_id="aws_default",  # Airflow AWS connection
        redshift_conn_id="redshift_default",  # Airflow Redshift connection
    )

    copy_with_manifest