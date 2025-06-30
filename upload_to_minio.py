from minio import Minio
from dotenv import load_dotenv
import os
from utils.logger import logger
import pandas as pd
from pathlib import Path
load_dotenv()  # Loads from .env in current directory by default


def get_minio_client(minio_access: str, minio_pass: str):
    client = Minio(
        "localhost:9000",
        access_key=minio_access,
        secret_key=minio_pass,
        secure=False,
    )
    return client


def remove_bucket(client, bucket_name: str):
    client.remove_bucket(bucket_name=bucket_name)


def can_upload_file(df_upload_log, object_name):
    if object_name in df_upload_log['File'].values:
        logger.info("File already uploaded")
        print("Checking in can upload file", df_upload_log)
        return False
    return True


def write_to_log(df_upload_log, object_name):
    new_row = pd.DataFrame(
        [{'File': object_name, 'Time': pd.Timestamp.now()}])
    df_upload_log = pd.concat(
        [df_upload_log, new_row], ignore_index=True, inplace=True)


def upload_file_to_minio(client, bucket_name: str, object_name: str, file_path: str, df_upload_log: str):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            client.fput_object(bucket_name, object_name, file_path)
        else:
            if can_upload_file(df_upload_log, object_name):
                client.fput_object(bucket_name, object_name, file_path)
            else:
                logger.info("File already uploaded")
                return
        write_to_log(df_upload_log, object_name)
        df_upload_log.to_csv('logs/upload_log.csv', index=False)
    except Exception as e:
        logger.error('Error from upload file to minio', e)


minio_access = os.getenv("MINIO_ROOT_USER")
minio_pass = os.getenv("MINIO_ROOT_PASSWORD")

client = get_minio_client(minio_access, minio_pass)
bucket_name = 'taxidata'
object_name = '2020/yellow_tripdata_2020-01.parquet'
file_path = 'data/2020/yellow_tripdata_2020-01.parquet'
if Path('logs/upload_log.csv').exists():
    df_upload_log = pd.read_csv('logs/upload_log.csv')
else:
    df_upload_log = pd.DataFrame(columns=['File', 'Time'])

upload_file_to_minio(client, bucket_name, object_name,
                     file_path, df_upload_log)
df_upload_log.to_csv('logs/upload_log.csv', index=False)
# go through all the files in data directory and upload to minio
# Pass 
