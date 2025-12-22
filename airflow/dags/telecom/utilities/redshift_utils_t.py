
import os
import psycopg2
import boto3
import json
import hashlib
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from botocore.exceptions import ClientError
from telecom.utilities.helper import boto3_destination_clients_init

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
    
    Args:
        sql: SQL query to execute
        fetch_results: Whether to fetch and return results
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

def table_has_recent_load(table: str, cutoff_ts: str = "1 day"):
    """
    Simple check: look if table has rows loaded in the last 24 hours.
    """
    sql = f"SELECT COUNT(1) FROM {table} WHERE load_timestamp >= SYSDATE - INTERVAL '{cutoff_ts}'"
    res = run_sql_script(sql, fetch_results=True)
    if res and res[0][0] > 0:
        return True
    return False

def initialize_file_tracking():
    """
    Initialize the file tracking table if it doesn't exist
    """
    sql = """
        CREATE TABLE IF NOT EXISTS test_raw_schema.file_tracking (
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
                            etag=obj['ETag'].strip('"')
                        ))
    except ClientError as e:
        print(f"Error listing S3 files: {e}")
        raise
    
    return files

def get_unprocessed_files(bucket: str, table_name: str, s3_prefix: str) -> Tuple[List[S3FileInfo], List[str]]:
    """
    Identify new or changed files that haven't been processed
    """
    # Get current S3 files
    current_files = get_s3_files_with_metadata(bucket, s3_prefix)
    if not current_files:
        return [], []
    
    # Get already processed files from Redshift
    sql = f"""
        SELECT file_path, file_etag, file_size
        FROM test_raw_schema.file_tracking
        WHERE table_name = '{table_name}'
    """
    processed_results = run_sql_script(sql, fetch_results=True)
    
    processed_files = {}
    if processed_results:
        for row in processed_results:
            processed_files[row[0]] = {'etag': row[1], 'size': row[2]}
    
    # Find unprocessed or changed files
    unprocessed_files = []
    file_urls = []
    
    for file_info in current_files:
        s3_url = f"s3://{bucket}/{file_info.key}"
        
        # Check if file is new or changed
        if s3_url not in processed_files:
            unprocessed_files.append(file_info)
            file_urls.append(s3_url)
        else:
            # Check if file has changed (etag or size)
            stored_info = processed_files[s3_url]
            if (file_info.etag != stored_info['etag'] or 
                file_info.size != stored_info['size']):
                unprocessed_files.append(file_info)
                file_urls.append(s3_url)
    
    return unprocessed_files, file_urls


def create_manifest_file(bucket: str, file_urls: List[str]) -> str:
    """
    Create Redshift-compatible manifest file (REQUIRES meta.content_length)
    """
    if not file_urls:
        return None

    s3_client = boto3_destination_clients_init()
    entries = []

    for url in file_urls:
        # Extract key from s3://bucket/key
        key = url.replace(f"s3://{bucket}/", "")

        # Get file size (MANDATORY for Redshift)
        head = s3_client.head_object(Bucket=bucket, Key=key)

        entries.append({
            "url": url,
            "mandatory": True,
            "meta": {
                "content_length": head["ContentLength"]
            }
        })

    manifest = {"entries": entries}

    manifest_key = (
        f"temp_manifests_folder/manifest_"
        f"{hashlib.md5(str(file_urls).encode()).hexdigest()[:8]}.json"
    )

    # Debug (keep this while stabilizing)
    print(f"DEBUG: Creating manifest with {len(entries)} entries")
    print("DEBUG: First entry:")
    print(json.dumps(entries[0], indent=2))

    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest),
        ContentType="application/json"
    )

    manifest_url = f"s3://{bucket}/{manifest_key}"
    print(f"DEBUG: Manifest created at: {manifest_url}")

    return manifest_url


def test_manifest_format(bucket: str, folder: str, prefix, file_name) -> Dict:
    """
    Test function to verify manifest format
    """
    test_files = [
        f"s3://{bucket}/{folder}/{prefix}/{file_name}.parquet"
        
    ]
    
    manifest = create_manifest_file(bucket, test_files)
    
    # Download and check the manifest
    s3_client = boto3_destination_clients_init()
    bucket, key = manifest.replace("s3://", "").split("/", 1)
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    
    print("Generated manifest:")
    print(json.dumps(json.loads(content), indent=2))
    
    # Verify it's valid JSON
    parsed = json.loads(content)
    assert "entries" in parsed
    assert len(parsed["entries"]) == 2
    
    for entry in parsed["entries"]:
        assert "url" in entry
        assert "mandatory" in entry
        print(f"Entry: {entry['url']}")
    
    return parsed

def verify_s3_urls(bucket: str, file_urls: List[str]) -> List[str]:
    """
    Verify that S3 URLs exist and are accessible
    """
    s3_client = boto3_destination_clients_init()
    valid_urls = []
    
    for url in file_urls:
        # Parse bucket and key from URL
        if not url.startswith("s3://"):
            print(f"WARNING: Invalid S3 URL format: {url}")
            continue
            
        # Remove s3:// prefix
        path = url[5:]  # Remove "s3://"
        url_bucket, key = path.split("/", 1)
        
        if url_bucket != bucket:
            print(f"WARNING: URL bucket mismatch: {url_bucket} != {bucket}")
            continue
        
        # Verify file exists
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            valid_urls.append(url)
            print(f"✓ Verified: {url}")
        except Exception as e:
            print(f"✗ File not accessible: {url} - Error: {e}")
    
    return valid_urls


def update_file_tracking(bucket: str, table_name: str, file_info: S3FileInfo, rows_loaded: int = 0):
    """
    Upsert file tracking info using Redshift MERGE
    (target table must NOT be aliased)
    """
    file_path = f"s3://{bucket}/{file_info.key}"
    file_path_escaped = file_path.replace("'", "''")
    etag_escaped = file_info.etag.replace("'", "''")

    sql = f"""
        MERGE INTO test_raw_schema.file_tracking
        USING (
            SELECT
                '{table_name}' AS table_name,
                '{file_path_escaped}' AS file_path,
                '{etag_escaped}' AS file_etag,
                {file_info.size} AS file_size,
                TIMESTAMP '{file_info.last_modified.strftime('%Y-%m-%d %H:%M:%S')}' AS last_modified,
                {rows_loaded} AS rows_loaded
        ) src
        ON test_raw_schema.file_tracking.table_name = src.table_name
       AND test_raw_schema.file_tracking.file_path = src.file_path
        WHEN MATCHED THEN
            UPDATE SET
                file_etag     = src.file_etag,
                file_size     = src.file_size,
                last_modified = src.last_modified,
                rows_loaded   = src.rows_loaded,
                processed_at  = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT (
                table_name,
                file_path,
                file_etag,
                file_size,
                last_modified,
                rows_loaded,
                processed_at
            )
            VALUES (
                src.table_name,
                src.file_path,
                src.file_etag,
                src.file_size,
                src.last_modified,
                src.rows_loaded,
                SYSDATE
            );
    """

    run_sql_script(sql)


def cleanup_old_manifests(bucket: str, days_to_keep: int = 7):
    """
    Clean up old manifest files from S3
    """
    s3_client = boto3_destination_clients_init()
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix='temp_manifests/'
        )
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        for obj in response.get('Contents', []):
            if obj['LastModified'] < cutoff_date:
                s3_client.delete_object(
                    Bucket=bucket,
                    Key=obj['Key']
                )
                print(f"Deleted old manifest: {obj['Key']}")
    except ClientError as e:
        print(f"Error cleaning up manifests: {e}")
        

def list_keys_with_prefix(bucket: str, prefix: str) -> list:
    """
    List all keys in S3 bucket with given prefix
    """
    s3_client = boto3_destination_clients_init()
    keys = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    keys.append(obj['Key'])
    except ClientError as e:
        print(f"Error listing S3 objects: {e}")
        raise
    
    return keys