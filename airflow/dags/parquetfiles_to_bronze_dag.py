from pathlib import Path

from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG, Dataset
import datetime
from airflow.operators.empty import EmptyOperator
from utils.config_loader import ConfigLoader

SPARK_CONFIG = {
    "jars": "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/iceberg-spark-runtime-3.4_2.12-1.5.2.jar",
    'conn_id': 'spark_conn',
    'total_executor_cores': '1',
    'executor_cores': '1',
    'executor_memory': '1g',
    'driver_memory': '1g',
    'num_executors': '1',
}

bronze_table = Dataset("s3:bronze_table")
parquet_files = Dataset("s3:parquet_files")
AIRFLOW_HOME = Path(Variable.get("AIRFLOW_HOME"))
app_path = AIRFLOW_HOME / "jobs" / "parquetfiles_to_bronze.py"

# TODO: refactor dag

# Init dag params
with DAG(
    dag_id="parquetfiles_to_bronze",
    description="A dag to load data from parquetfiles bucket to bronze table",
    schedule=[parquet_files],
    catchup=False,
    start_date=datetime.datetime(2021, 1, 1),
    tags=["ingestion"],
    params={"folders": [2015]},
) as dag:
    # Create task
    previous_task = None
    end_task = EmptyOperator(
        task_id="start_task", outlets=[bronze_table]
    )
    # Get dependency
    config = ConfigLoader()
    spark_config = config.get_spark_config()
    # Create task
    for i, folder in enumerate(dag.params["folders"]):
        submit_job = SparkSubmitOperator(
            task_id=f"parquet_to_bronze_{folder}",
            application=str(app_path.resolve()),
            application_args=[
                "--SRC_TABLE", str(folder),
                "--TARGET_TABLE", Variable.get("BRONZE_TABLE")
            ],
            conf=spark_config,
            **SPARK_CONFIG
        )
        # Schedule
        if previous_task:
            previous_task >> submit_job  # chain
        previous_task = submit_job
    submit_job >> end_task
