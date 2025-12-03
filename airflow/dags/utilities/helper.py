import pandas as pd
import boto3, os
import psycopg2
import awswrangler as wr
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any,List
from sqlalchemy import create_engine, text


load_dotenv()


def connect_to_db(user: str, password: str, host: str, port: str, database: str):
    """Establish a connection to the PostgreSQL database."""
    
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    return engine
def boto3_source_clients_init():
    s3 = boto3.client("s3",
                      aws_access_key_id=os.getenv('AWS_ACCES_KEY'),
                        aws_secret_access_key=os.getenv('AWS_SECRETE_KEY'),
                        region_name='eu-north-1')
    return s3

def boto3_destination_clients_init():
    s3 = boto3.client("s3",
                      aws_access_key_id=os.getenv('D_AWS_ACCES_KEY'),
                        aws_secret_access_key=os.getenv('D_AWS_SECRETE_KEY'),
                        region_name='eu-north-1')
    return s3

def s_session_init(aws_access_key, aws_secrete_key):
    session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secrete_key,
    region_name='eu-north-1')
    return session

def d_session_init(aws_access_key, aws_secrete_key):
    session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secrete_key,
    region_name='eu-north-1')
    return session

def s3_file_exists(bucket: str, key: str) -> bool: # USE THE DESTINATION BUCKET NAME
    s3 = boto3_destination_clients_init()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.NoSuchKey:
        return False
    except Exception:
        return False


def create_metadata(df: pd.DataFrame, source_type: str, source_details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create comprehensive metadata for the dataset
    
    Args:
        df: DataFrame to create metadata for
        source_type: Type of data source (google_sheet, s3_csv, database, etc.)
        source_details: Additional details about the source
    
    Returns:
        Dictionary containing metadata
    """
    current_time = datetime.utcnow()
    
    metadata = {
        # Load information
        "load_timestamp": current_time.isoformat() + "Z",
        "load_date": current_time.strftime("%Y-%m-%d"),
        "load_time": current_time.strftime("%H:%M:%S"),
        
        # Data source information
        "source_type": source_type,
        "source_details": source_details,
        
        # Data characteristics
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()}
        
       
    }
    
    return metadata

def save_to_parquet_with_metadata(df: pd.DataFrame, file_name: str, 
                                 source_type: str, source_details: Dict[str, Any],session,
                                 bucket_name: str = BUCKET_NAME,
                                 folder: str=Folder, metadata_folder: str=Meta_data_folder):
    """
    Save DataFrame to S3 as Parquet file with associated metadata
    
    Args:
        df: DataFrame to save
        file_name: Name of the parquet file (without extension)
        source_type: Type of data source
        source_details: Additional source details
        bucket_name: S3 bucket name
        folder: Central folder path in S3
    """
    try:
        # Create metadata
        metadata = create_metadata(df, source_type, source_details)
        
        # Add metadata as DataFrame attributes (will be saved in parquet metadata)
        df.attrs['source_metadata'] = metadata
        
        # Create S3 path for data
        data_s3_path = f"s3://{bucket_name}/{folder}/{file_name}.parquet"
        
        # Save as parquet with custom metadata
        wr.s3.to_parquet(
            df=df,
            path=data_s3_path,
            index=False,
            boto3_session=session,
            #compression='snappy',
            dataset=True
        )
        
        # Save separate metadata file
        metadata_s3_path = f"s3://{bucket_name}/{metadata_folder}/{file_name}_metadata.json"
        wr.s3.to_json(
            df=pd.DataFrame([metadata]),
            path=metadata_s3_path,
            boto3_session=session,
            orient='records',
            lines=True
        )
        
        print(f"✓ Successfully saved {file_name}.parquet to {data_s3_path}")
        print(f"✓ Metadata saved to {metadata_s3_path}")
        
        # Print summary
        print(f"  - Rows: {metadata['row_count']}, Columns: {metadata['column_count']}")
        print(f"  - Load Time: {metadata['load_timestamp']}")
        print(f"  - Source: {source_type}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error saving {file_name}.parquet: {str(e)}")
        return False

def list_keys_with_prefix(bucket: str, prefix: str) -> List[str]:
    s3 = boto3_destination_clients_init()
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def ensure_local_tmp(path: str = ".../tmp"):
    import os
    os.makedirs(path, exist_ok=True)
    return path

def create_log():
    pass