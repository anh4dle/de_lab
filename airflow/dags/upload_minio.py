from datetime import datetime, timedelta
import random
from airflow.decorators import dag, task
import pendulum
DAG_ID = 'upload_to_minio'
CRON_SCHEDULE = "*/10 * * * *"
TIMEZONE = "Asia/Ho_Chi_Minh"

# A dictionary contains config that applied to all tasks.
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2015, 12, 1, tz="UTC"),
    "retries": 1
}


@dag(
    DAG_ID,
    schedule=CRON_SCHEDULE,
    description='A simple DAG to generate and check random numbers',
    default_args=DEFAULT_ARGS,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    max_consecutive_failed_dag_runs=2
)
def task_flow():
    @task
    def generate_task():
        number = random.randint(1, 100)
        print(f"Generated random number: {number}")
        return number

    @task
    def check_task(number):
        result = "even" if number % 2 == 0 else "odd"
        print(f"The number {number} is {result}.")

    check_task(generate_task())


task_flow()
