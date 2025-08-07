from pathlib import Path
from datetime import timedelta
from airflow.decorators import dag, task
import pendulum
from jobs.download_and_upload import submit_download_and_upload
from utils.minio_utils import MinIOWrapper
from airflow.models import Variable
import asyncio
import csv

DAG_ID = 'ingest_parquetfiles'
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
    description='A DAG to download taxi data files and upload to bucket parquetfiles on minio',
    default_args=DEFAULT_ARGS,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    max_consecutive_failed_dag_runs=2
)
def task_flow():
    @task
    def download():
        current_year = 2020
        end_year = 2022
        bucket_name = 'lake'
        minio_url, minio_access, minio_pass = Variable.get("MINIO_URL"), Variable.get(
            "MINIO_ROOT_USER"), Variable.get("MINIO_ROOT_PASSWORD")
        minio = MinIOWrapper(minio_url, minio_access, minio_pass)

        with open(LOG_FILE_PATH, 'a') as f:
            csv_writer = csv.writer(f)
            if Path(LOG_FILE_PATH).stat().st_size == 0:
                header = ['File', 'Error', 'Time']
                csv_writer.writerow(header)

        asyncio.run(submit_download_and_upload(minio, bucket_name, current_year, end_year,
                    csv_writer))
    download()


dag = task_flow()
