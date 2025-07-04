from minio import Minio
import os
from dotenv import load_dotenv
from pathlib import Path


def get_minio_client(minio_access: str, minio_pass: str):
    client = Minio(
        "localhost:9000",
        access_key=minio_access,
        secret_key=minio_pass,
        secure=False,
    )
    return client


def get_downloaded_files_by_year(minio, bucket, year):
    prefix = f"{year}/"  # Ensure it's a string with a trailing slash

    objects = minio.list_objects(bucket, prefix=prefix, recursive=True)
    result = [object.object_name for object in objects]
    return result


def create_bucket(minio_client: Minio, bucket_name: str):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)


def upload_stream_obj(minio_client: Minio, bucket_name: str, object_name: str, data):
    minio_client.put_object(bucket_name, object_name, data)


def main():
    minio_access, minio_pass = os.getenv(
        'MINIO_ROOT_USER'), os.getenv('MINIO_ROOT_PASSWORD')
    minio_client = get_minio_client(minio_access, minio_pass)
    bucket = 'lake'
    create_bucket(minio_client, 'lake')
    print(get_downloaded_files_by_year(minio_client, bucket, 2020))


main()
