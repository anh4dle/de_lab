from minio import Minio
from pathlib import Path
from dotenv import load_dotenv
import os
from utils.logger import logger

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

def check_before_upload(client, bucket_name, object_name):
    try:
        client.stat_object(bucket_name, object_name)
        logger.info(f"{object_name} already exists.")
    except Exception as e:
        logger.error(e)

def upload_file_to_minio(client, bucket_name: str, object_name: str, file_path: str):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        client.fput_object(bucket_name, object_name, file_path)
    except Exception as e:
        print(e)


minio_access = os.getenv("MINIO_ROOT_USER")
minio_pass = os.getenv("MINIO_ROOT_PASSWORD")

client = get_minio_client(minio_access, minio_pass)
upload_file_to_minio("taxidata", "2020/yellow_tripdata_2020-01.parquet", "data/2020/yellow_tripdata_2020-01.parquet")