# dags/helpers/redshift_utils.py
import os
import psycopg2
import boto3
import json
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass
from botocore.exceptions import ClientError
from utilities.helper import boto3_destination_clients_init
import hashlib

@dataclass
class S3FileInfo:
    key: str
    size: int
    last_modified: datetime
    etag: str

def get_redshift_conn_info() -> Dict[str, str]:
    """
    Get Redshift connection information from environment variables
    """
    return {
        "host": os.environ.get("RDST_HOST"),
        "port": os.environ.get("RDST_PORT", "5439"),
        "dbname": os.environ.get("RDST_DB"),
        "user": os.environ.get("RDST_USER"),
        "password": os.environ.get("RDST_PASSWORD")
    }

def run_sql_script(sql: str, fetch_results: bool = False):
    """
    Execute SQL script with proper connection handling
    """
    conn_info = get_redshift_conn_info()
    conn = None
    try:
        conn = psycopg2.connect(
            host=conn_info["host"],
            port=conn_info["port"],
            dbname=conn_info["dbname"],
            user=conn_info["user"],
            password=conn_info["password"]
        )
        cur = conn.cursor()
        cur.execute(sql)
        
        if fetch_results:
            try:
                results = cur.fetchall()
            except Exception:
                results = None
        else:
            results = None
            
        conn.commit()
        cur.close()
        return results
        
    except Exception as e:
        print(f"SQL execution error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def initialize_file_tracking():
    """
    Initialize the file tracking table if it doesn't exist
    """
    sql = """
        CREATE TABLE IF NOT EXISTS raw_schema.file_tracking (
            table_name VARCHAR(100),
            file_path VARCHAR(500),
            file_etag VARCHAR(50),
            file_size BIGINT,
            last_modified TIMESTAMP,
            processed_at TIMESTAMP DEFAULT SYSDATE,
            rows_loaded INTEGER,
            PRIMARY KEY (table_name, file_path)
        )
    """
    run_sql_script(sql)

# def get_s3_client():
#     """Get S3 client"""
#     return boto3.client('s3')

def get_s3_files_with_metadata(bucket: str, prefix: str) -> List[S3FileInfo]:
    """
    Get all S3 files with metadata for change detection
    """
    s3_client = boto3_destination_clients_init()
    files = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Only process Parquet files
                    if obj['Key'].lower().endswith('.parquet'):
                        files.append(S3FileInfo(
                            key=obj['Key'],
                            size=obj['Size'],
                            last_modified=obj['LastModified'],
                            etag=obj['ETag'].strip('"')  # Remove quotes
                        ))
    except ClientError as e:
        print(f"Error listing S3 files: {e}")
        raise
    
    return files

def get_processed_files(table_name: str) -> Dict:
    """
    Get already processed files from Redshift
    """
    sql = f"""
        SELECT file_path, file_etag, file_size
        FROM raw_schema.file_tracking
        WHERE table_name = '{table_name}'
    """
    processed_results = run_sql_script(sql, fetch_results=True)
    
    processed_files = {}
    if processed_results:
        for row in processed_results:
            processed_files[row[0]] = {'etag': row[1], 'size': row[2]}
    
    return processed_files

def update_file_tracking(bucket: str, table_name: str, file_info: S3FileInfo, rows_loaded: int = 0):
    """
    Update file tracking table after successful load
    """
    file_path = f"s3://{bucket}/{file_info.key}"
    
    # Escape single quotes
    file_path_escaped = file_path.replace("'", "''")
    
    sql = f"""
        INSERT INTO raw_schema.file_tracking 
        (table_name, file_path, file_etag, file_size, last_modified, rows_loaded)
        VALUES (
            '{table_name}', 
            '{file_path_escaped}', 
            '{file_info.etag}', 
            {file_info.size}, 
            '{file_info.last_modified.strftime('%Y-%m-%d %H:%M:%S')}', 
            {rows_loaded}
        )
        ON CONFLICT (table_name, file_path) 
        DO UPDATE SET 
            file_etag = EXCLUDED.file_etag,
            file_size = EXCLUDED.file_size,
            last_modified = EXCLUDED.last_modified,
            processed_at = SYSDATE,
            rows_loaded = EXCLUDED.rows_loaded
    """
    
    run_sql_script(sql)
