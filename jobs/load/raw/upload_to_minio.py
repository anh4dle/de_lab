from minio.minio import Minio
from dotenv import load_dotenv
import os
from utils.logger import logger
import pandas as pd
from pathlib import Path
load_dotenv()  # Loads from .env in current directory by default


def get_minio_client(minio_access: str, minio_pass: str):
    client = Minio(
        "minio:9000",
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
