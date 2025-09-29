
from datetime import timedelta

from airflow.operators.python import PythonOperator

import pendulum
from jobs.ingest_parquetfiles import submit_download_and_upload
from airflow.models import Variable
import asyncio

from airflow import DAG


# A dictionary contains config that applied to all tasks.
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2015, 12, 1, tz="UTC"),
    "retries": 1
}


def download(**context):
    start_year = context["params"]["start_year"]
    end_year = context["params"]["end_year"]
    bucket_name = 'lake'
    minio_url, minio_access, minio_pass = Variable.get("MINIO_URL"), Variable.get(
        "MINIO_ROOT_USER"), Variable.get("MINIO_ROOT_PASSWORD")

    asyncio.run(submit_download_and_upload(minio_url, minio_access,
                                           minio_pass, bucket_name, start_year, end_year))


with DAG(
    dag_id='ingest_parquetfiles',
    schedule="*/120 * * * *",
    description='A DAG to download taxi data files and upload to bucket parquetfiles on minio',
    default_args=DEFAULT_ARGS,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    max_consecutive_failed_dag_runs=2,
    params={"start_year": 2012, "end_year": 2014}
) as dag:

    download_task = PythonOperator(
        task_id="download_taxi_data",
        python_callable=download,
        provide_context=True,
    )
    download_task
