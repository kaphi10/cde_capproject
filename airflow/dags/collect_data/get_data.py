import boto3
import pandas as pd
import psycopg2
import json, os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import awswrangler as wr
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from sqlalchemy import text
import warnings
warnings.filterwarnings("ignore")
import sys

current_dir = Path(__file__).parent
project_root = current_dir.parent if current_dir.name != 'Data_ETL' else current_dir
sys.path.append(str(project_root))

from utilities.helper import s3_file_exists, boto3_source_clients_init,  boto3_destination_clients_init, session_init
load_dotenv()

import os




os.makedirs('/tmp', exist_ok=True)

def extract_webforms(schema, bucket, conn):
    # bucket = "coretelecom-raw-bucket"
    des_s3 = boto3_destination_clients_init()
    
    # Use SQLAlchemy's text() construct
    try:
        result = conn.execute(text(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
        """))
        table_names = [row[0] for row in result]

        
        print(f"Found tables in schema '{schema}': {table_names}")

        for t in table_names:
            output_key = f"raw_data/webforms/{t}.parquet"

            if s3_file_exists(bucket, output_key):
                print(f"[SKIP] webform {t} already exists.")
                continue
            
            result = conn.execute(text(f"SELECT * FROM {schema}.{t}"))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            df.columns = [c.lower().replace(" ", "") for c in df.columns]
            df=df.drop(columns=['column1'])
            #df.columns = [c.lower().replace(" ", "") for c in df.columns]
            df["load_timestamp"] = datetime.utcnow()
            df = df.astype({
                "request_id": "string",
                "customerid": "string",
                "agentid": "string",
                "complaint_category": "string",
                "resolutionstatus": "string",
                "request_date": "datetime64[ns]",
                "resolution_date": "datetime64[ns]",
                "webformgenerationdate": "datetime64[ns]",
            })
                        
                
                
            temp_file=f'/tmp/{t}.parquet'

            df.to_parquet(temp_file)
            des_s3.upload_file(temp_file, bucket, output_key)
            print(f"[UPLOADED] webform {t} to s3://{bucket}/{output_key}")
            
             # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        raise
        

    print("[DONE] Webforms extraction completed")
    

def extract_customers(source_bucket, dest_bucket, prefix='customers/', folder='customers'):
    """
    Extract call center data from source bucket and load to destination bucket
    
    Args:
        source_bucket (str): Source S3 bucket name
        dest_bucket (str): Destination S3 bucket name
    """
    
    # Initialize sessions for both accounts
    source_session = session_init(profile_name='source')  # Replace with your source profile
    dest_session = session_init(profile_name='destination')    # Replace with your dest profile
    
    # Initialize boto3 clients
    s3_source = boto3_source_clients_init()       # For listing/checking files
    s3_dest = boto3_destination_clients_init()    # For uploading files
    
    # prefix = 'call logs/'
    
    # List objects in source bucket
    try:
        resp = s3_source.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
        
        if 'Contents' not in resp:
            print(f"No files found in {source_bucket}/{prefix}")
            return
        
        for obj in resp.get("Contents", []):
            csv_key = obj["Key"]
            
            # Skip if not a CSV file
            if not csv_key.lower().endswith('.csv'):
                continue
                
            date_part = csv_key.split("/")[-1].replace(".csv", "")
            output_key = f"raw_data/{folder}/{date_part}.parquet"
            
            # Check if file already exists in destination (using destination session)
            if s3_file_exists(dest_bucket, output_key):
                print(f"[Skip] {folder} file {output_key} already exists")
                continue
            
            # Read CSV from source using awswrangler with source session
            s3_file_path = f"s3://{source_bucket}/{csv_key}"
            
            print(f"Processing: {s3_file_path}")
            
            # Read CSV using awswrangler with the source session
            df = wr.s3.read_csv(
                path=s3_file_path,
                boto3_session=source_session  # Pass source session for reading
            )
            
            # Clean column names
            df.columns = [c.lower().replace(" ", "") for c in df.columns]
            df["load_timestamp"] = datetime.utcnow()               
            df['dateofbirth']=pd.to_datetime(df['dateofbirth'])
            df['dateofbirth']=df['dateofbirth'].dt.date
            df['signup_date']=pd.to_datetime(df['signup_date'])
            df['signup_date']=df['signup_date'].dt.date   
            
            print(f"✓ Loaded {prefix}: {len(df)} rows from {csv_key}")
            
            # Save locally as parquet
            temp_file = f"/tmp/customers_{date_part}.parquet"
            df.to_parquet(temp_file)
            
            # Upload to destination using boto3 with destination client
            s3_dest.upload_file(temp_file, dest_bucket, output_key)
            print(f'✓ File {output_key} transferred to S3')
            
             # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        raise
    
def extract_call_logs(source_bucket, dest_bucket, prefix="call logs/", folder="callcenters"):

    source_session = session_init(profile_name='source')
    dest_session = session_init(profile_name='destination')

    s3_source = boto3_source_clients_init()
    s3_dest = boto3_destination_clients_init()
    
    CALL_LOG_SCHEMA = [
    "callid",
    "customerid",
    "agentid",
    "complaint_category",
    "resolutionstatus",
    "call_start_time",
    "call_end_time",
    "calllogsgenerationdate",
    "load_timestamp"
    ]
    try:
        resp = s3_source.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
        if "Contents" not in resp:
            print(f"No files found in {source_bucket}/{prefix}")
            return

        for obj in resp["Contents"]:
            csv_key = obj["Key"]
            if not csv_key.endswith(".csv"):
                continue

            date_part = csv_key.split("/")[-1].replace(".csv", "")
            output_key = f"raw_data/{folder}/{date_part}.parquet"

            # Skip if already exists
            if s3_file_exists(dest_bucket, output_key):
                print(f"[Skip] {output_key} already exists")
                continue

            print(f"Processing {csv_key} ...")

            df = wr.s3.read_csv(
                path=f"s3://{source_bucket}/{csv_key}",
                boto3_session=source_session
            )

            # Normalize column names
            df.columns = (
                df.columns
                .str.lower()
                .str.replace(" ", "", regex=False)
                .str.replace(":", "", regex=False)
            )
            df["load_timestamp"] = datetime.utcnow()

            # Drop any extra unexpected columns
            df = df[[c for c in df.columns if c in CALL_LOG_SCHEMA]]

            # Enforce column presence
            for col in CALL_LOG_SCHEMA:
                if col not in df.columns:
                    df[col] = None

            # Reorder
            df = df[CALL_LOG_SCHEMA]
            df = df.astype({
                "callid": "string",
                "customerid": "string",
                "agentid": "string",
                "complaint_category": "string",
                "resolutionstatus": "string",
                "call_start_time": "datetime64[ns]",
                "call_end_time": "datetime64[ns]",
                "calllogsgenerationdate": "datetime64[ns]",
            })

            # # Cast datetimes
            df["call_start_time"] = pd.to_datetime(df["call_start_time"], errors="coerce")
            df["call_end_time"] = pd.to_datetime(df["call_end_time"], errors="coerce")
            df["calllogsgenerationdate"] = pd.to_datetime(df["calllogsgenerationdate"], errors="coerce").dt.date
            

            temp_file = f"/tmp/callcenter_{date_part}.parquet"
            
            df.to_parquet(
                temp_file,
                index=False,  # MUST BE FALSE
                engine='pyarrow',
                coerce_timestamps='ms',
                allow_truncated_timestamps=True
            )

            s3_dest.upload_file(temp_file, dest_bucket, output_key)
            print(f"✓ Uploaded standardized Parquet {output_key}")
            
        if os.path.exists(temp_file):
                os.remove(temp_file)
            
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        raise


def extract_socialmedia(source_bucket, dest_bucket, prefix, folder):
    """
    Extract call center data from source bucket and load to destination bucket
    
    Args:
        source_bucket (str): Source S3 bucket name
        dest_bucket (str): Destination S3 bucket name
    """
    
    # Initialize sessions for both accounts
    source_session = session_init(profile_name='source')  # Replace with your source profile
    dest_session = session_init(profile_name='destination')    # Replace with your dest profile
    
    # Initialize boto3 clients
    s3_source = boto3_source_clients_init()       # For listing/checking files
    s3_dest = boto3_destination_clients_init()    # For uploading files
    
    # prefix = 'call logs/'
    
    # List objects in source bucket
    try:
        resp = s3_source.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
        
        if 'Contents' not in resp:
            print(f"No files found in {source_bucket}/{prefix}")
            return
        
        for obj in resp.get("Contents", []):
            json_key = obj["Key"]
            
            # Skip if not a json file
            if not json_key.lower().endswith('.json'):
                continue
                
            date_part = json_key.split("/")[-1].replace(".json", "")
            output_key = f"raw_data/{folder}/{date_part}.parquet"
            
            # Check if file already exists in destination (using destination session)
            if s3_file_exists(dest_bucket, output_key):
                print(f"[Skip] social media file {output_key} already exists")
                continue
            
            # Read CSV from source using awswrangler with source session
            s3_file_path = f"s3://{source_bucket}/{json_key}"
            
            print(f"Processing: {s3_file_path}")
            
            # Read CSV using awswrangler with the source session
            df = wr.s3.read_json(
                path=s3_file_path,
                boto3_session=source_session  # Pass source session for reading
            )
            
            # Clean column names
            df.columns = [c.lower().replace(" ", "") for c in df.columns]
            df["load_timestamp"] = datetime.utcnow()
            # df.drop(columns=[''])
             # Drop any extra unexpected columns
            MEDIA_SCHEMA = ['complaint_id','customerid','agentid',
                            'complaint_category','resolutionstatus', 'request_date', 
                            'resolution_date', 'media_channel',
                            'mediacomplaintgenerationdate','load_timestamp']
            df = df[[c for c in df.columns if c in MEDIA_SCHEMA]]

            # Enforce column presence
            for col in MEDIA_SCHEMA:
                if col not in df.columns:
                    df[col] = None

            # Reorder
            df = df[MEDIA_SCHEMA]
            
            
            df['agentid']=df['agentid'].astype(str)
            df['request_date']=pd.to_datetime(df['request_date'])
            df['resolution_date']=pd.to_datetime(df['resolution_date'])
            df['mediacomplaintgenerationdate']=pd.to_datetime(df['mediacomplaintgenerationdate'])
            df['mediacomplaintgenerationdate']=df['mediacomplaintgenerationdate'].dt.date
            
            print(f"✓ Loaded call logs: {len(df)} rows from {json_key}")
            
            # Save locally as parquet
            temp_file = f"/tmp/json_{date_part}.parquet"
            df.to_parquet(temp_file) 
            # Upload to destination using boto3 with destination client
            s3_dest.upload_file(temp_file, dest_bucket, output_key)
            print(f'✓ File {output_key} transferred to S3')
              # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        raise
        
def extract_agents(bucket):
    
    # today = str(datetime.utcnow().date())
    output_key = f"raw_data/agents/agents.parquet"

    # --- IDEMPOTENCY ---
    if s3_file_exists(bucket, output_key):
        print(f"[SKIP] agents file  already exists.")
        return
    
    scope = ['https://www.googleapis.com/auth/spreadsheets']

    # Path to your service account JSON key file
    creds_file = os.getenv("KEY_PATH")

    # Authenticate with Google Sheets
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet by its title
    sheet_title = os.getenv("SHEET_TITLE")
    sheet_id = os.getenv("SHEET_ID")
    
   
    spreadsheet = client.open_by_key(sheet_id).worksheet(sheet_title)

    # Extract data
    all_data = spreadsheet.get_all_values()  # Get all values as a list of lists
    df=pd.DataFrame(all_data[1:], columns=all_data[0])
    #df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df["load_timestamp"] = datetime.utcnow()
    
     # Save locally as parquet
    temp_file = f"/tmp/csv_{sheet_title}.parquet"
    df.to_parquet(temp_file) 

    dest_s3 = boto3_destination_clients_init()
    dest_s3.upload_file(temp_file, bucket, output_key)
    print(f'✓ File {output_key} transferred to S3')
              # Clean up temp file
    if os.path.exists(temp_file):
         os.remove(temp_file)

    print("[DONE] Agents extraction complete.")

 
# def extract_customers(source_bucket,dest_bucket,session):
#     s3 = boto3_source_clients_init()
#     print("SOURCE BUCKET:", source_bucket)
#     print("DEST BUCKET:", dest_bucket)
#     # bucket = "coretelecom-raw-bucket"

#     #today = str(datetime.utcnow().date())
#     prefix='customers'
    
#     resp=s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix) # list items in the source bucket
#     for obj in resp.get("Contents", []):
#         csv_key=obj["Key"]
#         date_part=csv_key.split("/")[-1].replace(".csv","")
#         output_key=f"raw/customers/{date_part}.parquet"

#         # --- IDEMPOTENCY CHECK ---
#         if s3_file_exists(dest_bucket, output_key):
#             print(f"[SKIP] customers file  already exists.")
#             return  
        
#         s3_file_path=f"s3://{source_bucket}/{csv_key}"
#         df= wr.s3.read_csv(
#             path=s3_file_path, 
#             boto3_session=session
#         )
#         #df.columns = [c.lower().replace(" ", "_") for c in df.columns]
#         df["load_timestamp"] = datetime.utcnow()
        
#         # Save Parquet
#         df.to_parquet("../temp/customers.parquet")

#         # Upload
#         # initiate destination s3 client
#         dest_s3 = boto3_destination_clients_init()
#         dest_s3.upload_file("../temp/customers.parquet", dest_bucket, output_key)

#     print("[DONE] Customers extraction completed.")

        
            
    
    
