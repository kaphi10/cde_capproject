import pandas as pd
import boto3, os
import psycopg2
import awswrangler as wr
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any,List
from sqlalchemy import create_engine, text
from botocore.exceptions import ClientError, NoCredentialsError

load_dotenv()


def connect_to_db(user: str, password: str, host: str, port: str, database: str):
    """Establish a connection to the PostgreSQL database."""
    
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string, pool_size=10,  # Adjust based on your needs
                        max_overflow=20,  # Additional connections beyond pool_size
                        pool_recycle=3600,  # Recycle connections after 1 hour
                        pool_pre_ping=True)
    return engine

# def s_session_init():
#     aws_access_key=os.getenv('S_AWS_ACCES_KEY')
#     aws_secrete_key=os.getenv('S_AWS_SECRETE_KEY')
#     session = boto3.Session(
#     aws_access_key_id=aws_access_key,
#     aws_secret_access_key=aws_secrete_key,
#     region_name='eu-north-1')
#     return session

def boto3_source_clients_init():
    """Initialize source AWS account clients (using source profile)"""
    source_session = session_init(profile_name='source')  # Your source AWS profile
    s3_client = source_session.client('s3')
    return s3_client

def boto3_destination_clients_init():
    """Initialize destination AWS account clients (using destination profile)"""
    dest_session = session_init(profile_name='destination')  # Your destination AWS profile
    s3_client = dest_session.client('s3')
    return s3_client

def session_init(profile_name, region_name='eu-north-1'):
    """Initialize boto3 session with fallback options"""
    try:
        if profile_name:
            session = boto3.Session(profile_name=profile_name, region_name=region_name)
            # Test the session
            sts = session.client('sts')
            identity = sts.get_caller_identity()
            print(f"✓ Session initialized for profile '{profile_name}': {identity['Arn']}")
        else:
            
            print(f"✓ Session initialized with default profile: profile name is none")
        
        return session
        
    except (NoCredentialsError, ClientError) as e:
        print(f"Error initializing session for profile '{profile_name}': {str(e)}")
        raise
def s3_file_exists(bucket: str, key: str) -> bool: # USE THE DESTINATION BUCKET NAME
    s3 = boto3_destination_clients_init()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.NoSuchKey:
        return False
    except Exception:
        return False
    
def save_data_to_s3(df: pd.DataFrame, des_session, outputpath, bucket_name):
    
    try:
        # Create S3 path for data
        data_s3_path = f"s3://{bucket_name}/{outputpath}"
        # Save as parquet with custom metadata
        wr.s3.to_parquet(
            df=df,
            path=data_s3_path,
            index=False,
            #database='project_glue_database',
            boto3_session=des_session,
            dataset=False
        )
        
        return True
    except Exception as e:
        print(f"Error in transfering data to {data_s3_path} with error {e}")
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