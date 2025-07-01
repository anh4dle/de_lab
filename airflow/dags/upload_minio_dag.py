from pathlib import Path
import os
import pandas as pd
from airflow.models import Variable
from datetime import datetime, timedelta
import random
from airflow.decorators import dag, task
import pendulum
from jobs.load.raw.upload_to_minio import upload_file_to_minio, get_minio_client

DAG_ID = 'upload_to_minio'
CRON_SCHEDULE = "*/10 * * * *"
TIMEZONE = "Asia/Ho_Chi_Minh"
LOG_FILE_PATH = 'logs/failed_download.csv'
# A dictionary contains config that applied to all tasks.
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2015, 12, 1, tz="UTC"),
    "retries": 1
}


@dag(
    DAG_ID,
    schedule=CRON_SCHEDULE,
    description='A DAG to upload file from disk to minios',
    default_args=DEFAULT_ARGS,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    max_consecutive_failed_dag_runs=2
)
def task_flow():
    @task
    def upload():
        minio_access = Variable.get("MINIO_ROOT_USER")
        minio_pass = Variable.get("MINIO_ROOT_PASSWORD")

        client = get_minio_client(minio_access, minio_pass)
        bucket_name = 'taxidata'
        object_name = '2020/yellow_tripdata_2020-04.parquet'
        file_path = 'data/2020/yellow_tripdata_2020-04.parquet'
        if Path('logs/upload_log.csv').exists():
            df_upload_log = pd.read_csv('logs/upload_log.csv')
        else:
            df_upload_log = pd.DataFrame(columns=['File', 'Time'])

        upload_file_to_minio(client, bucket_name, object_name,
                             file_path, df_upload_log)
        df_upload_log.to_csv('logs/upload_log.csv', index=False)

    upload()


task_flow()
