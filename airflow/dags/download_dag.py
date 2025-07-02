from pathlib import Path
from datetime import timedelta
from airflow.decorators import dag, task
import pendulum
from jobs.extract.extract import extract
import asyncio
import csv

DAG_ID = 'download_dag'
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
    description='A DAG to download taxi data files',
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
        downloaded_files = {f.name for f in Path(
            'data').rglob('*') if f.is_file()}

        with open(LOG_FILE_PATH, 'a') as f:
            csv_writer = csv.writer(f)
            if Path(LOG_FILE_PATH).stat().st_size == 0:
                header = ['File', 'Error', 'Time']
                csv_writer.writerow(header)

        async def run_extract():
            await extract(current_year, end_year, csv_writer, downloaded_files)
        asyncio.run(run_extract())
    download()


dag = task_flow()
