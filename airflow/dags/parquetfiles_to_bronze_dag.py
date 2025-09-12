from pathlib import Path
import datetime
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG


SPARK_CONFIG = {
    "jars": "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/iceberg-spark-runtime-3.4_2.12-1.5.2.jar",
    'conn_id': 'spark_conn',
    'total_executor_cores': '1',
    'executor_cores': '1',
    'executor_memory': '1g',
    'driver_memory': '1g',
    'num_executors': '1',
}

AIRFLOW_HOME = Path(Variable.get("AIRFLOW_HOME"))

app_path = AIRFLOW_HOME / "jobs" / "parquetfiles_to_bronze.py"


with DAG(
    dag_id="parquetfiles_to_bronze",
    start_date=datetime.datetime(2021, 1, 1),
    description="A dag to load data from parquetfiles bucket to bronze table",
    # schedule="@daily",
    catchup=False,
    tags=["ingestion"],

) as dag:
    # Cannot use inside taskflow API so we use context manager
    # Parquet file path will change from root folder level to year level.
    folders = [2015, 2016, 2017]
    previous_task = None

    for i, folder in enumerate(folders):
        print("Inside spark submit")
        submit_job = SparkSubmitOperator(
            task_id=f"parquet_to_bronze_{folder}",
            application=str(app_path.resolve()),
            application_args=[
                # "--SRC_TABLE", Variable.get("PARQUETFILES_PATH"),
                "--SRC_TABLE", str(folder),
                "--TARGET_TABLE", Variable.get("BRONZE_TABLE"),
                "--spark_config_path", Variable.get("SPARK_CONFIG_PATH"),
            ],
            conf={
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.sql.parquet.enableVectorizedReader': "false"
            },
            **SPARK_CONFIG
        )
        if previous_task:
            previous_task >> submit_job  # chain
        previous_task = submit_job
