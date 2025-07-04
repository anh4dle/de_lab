from minio import Minio
import os
from dotenv import load_dotenv


def get_minio_client(minio_access: str, minio_pass: str):
    client = Minio(
        "localhost:9000",
        access_key=minio_access,
        secret_key=minio_pass,
        secure=False,
    )
    return client


def create_bucket(minio_client: Minio, bucket_name: str):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)


def main():
    minio_access, minio_pass = os.getenv(
        'MINIO_ROOT_USER'), os.getenv('MINIO_ROOT_PASSWORD')
    minio_client = get_minio_client(minio_access, minio_pass)

    create_bucket(minio_client, 'lake')


main()
