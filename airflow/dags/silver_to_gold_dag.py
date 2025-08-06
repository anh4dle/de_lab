from pathlib import Path
import datetime
from airflow.decorators import dag, task
import os
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from airflow import DAG

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2015, 12, 1, tz="UTC"),
    "retries": 1
}

SPARK_CONFIG = {
    'packages': "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    'conn_id': 'spark_conn',
    'total_executor_cores': '1',
    'executor_cores': '1',
    'executor_memory': '4g',
    'num_executors': '1',
}

with DAG(
    dag_id="silver_to_gold",
    start_date=datetime.datetime(2021, 1, 1),
    description="A dag to load data from silver to gold table",
    schedule="@daily",
    catchup=False,
    tags=["ingestion"],
    default_args=DEFAULT_ARGS,
) as dag:
    AIRFLOW_HOME = Path(Variable.get("AIRFLOW_HOME"))
    app_path = AIRFLOW_HOME / "jobs" / "load" / "silver_to_gold.py"

    # Cannot use inside taskflow API so we use context manager
    submit_job = SparkSubmitOperator(
        task_id="parquet_to_bronze",
        application=str(app_path.resolve()),
        application_args=[
            "--SRC_TABLE", Variable.get("SILVER_TABLE"),
            "--TARGET_TABLE", Variable.get("GOLD_TABLE"),
            "--spark_config_path", Variable.get("SPARK_CONFIG_PATH"),
        ],
        conf={
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
        },
        **SPARK_CONFIG
    )
