from pathlib import Path
import datetime
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG, Dataset

silver_table = Dataset("s3:silver_table")

SPARK_CONFIG = {
    "jars": "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/iceberg-spark-runtime-3.4_2.12-1.5.2.jar",
    'conn_id': 'spark_conn',
    'total_executor_cores': '1',
    'executor_cores': '1',
    'executor_memory': '2g',
    'driver_memory': '512m',
    'num_executors': '1',
}

with DAG(
    dag_id="silver_to_gold",
    start_date=datetime.datetime(2021, 1, 1),
    description="A dag to load data from silver to gold table",
    schedule=[silver_table],
    catchup=False,
    tags=["ingestion"]

) as dag:
    AIRFLOW_HOME = Path(Variable.get("AIRFLOW_HOME"))
    app_path = AIRFLOW_HOME / "jobs" / "silver_to_gold.py"

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
