from pathlib import Path
import pandas as pd
from airflow.models import Variable
from datetime import timedelta
from airflow.decorators import dag, task
import pendulum
from jobs.load.raw.upload_to_minio import upload_file_to_minio, get_minio_client
from utils.file_path import get_all_files_in_dir

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
    def get_minio():
        minio_access = Variable.get("MINIO_ROOT_USER")
        minio_pass = Variable.get("MINIO_ROOT_PASSWORD")
        client = get_minio_client(minio_access, minio_pass)
        return client

    @task
    def get_path_and_obj_names(airflow_home):

        file_dict = get_all_files_in_dir(airflow_home)
        file_paths = [Path(airflow_home) / k / v for k, v in file_dict.items()]
        object_names = [k / v for k,
                        v in file_dict.items()]
        return file_paths, object_names

    @task
    def upload():
        airflow_home = Variable.get("AIRFLOW_HOME")
        minio_client = get_minio()
        bucket_name = Variable.get("MINIO_BUCKET")

        file_paths, object_names = get_path_and_obj_names(airflow_home)

        if Path('logs/upload_log.csv').exists():
            df_upload_log = pd.read_csv('logs/upload_log.csv')
        else:
            df_upload_log = pd.DataFrame(columns=['File', 'Time'])

        for file_path, object_name in zip(file_paths, object_names):
            upload_file_to_minio(minio_client, bucket_name, object_name,
                                 file_path, df_upload_log)

        df_upload_log.to_csv('logs/upload_log.csv', index=False)

    get_minio() >> upload()


task_flow()
