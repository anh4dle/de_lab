from pathlib import Path
import datetime
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG, Dataset
from utils.config_loader import ConfigLoader
import pendulum

bronze_table = Dataset("s3:bronze_table")
silver_table = Dataset("s3:silver_table")

SPARK_DRIVER_ONFIG = {
    "jars": "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/iceberg-spark-runtime-3.4_2.12-1.5.2.jar",
    'conn_id': 'spark_conn',
    'total_executor_cores': '1',
    'executor_cores': '1',
    'executor_memory': '2g',
    'driver_memory': '512m',
    'num_executors': '1',
}

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2015, 12, 1, tz="UTC"),
    "retries": 1
}

AIRFLOW_HOME = Path(Variable.get("AIRFLOW_HOME"))
app_path = AIRFLOW_HOME / "jobs" / "bronze_to_silver.py"

# Cannot use SparkSubmitOperator inside taskflow API so we use context manager

with DAG(
    dag_id="bronze_to_silver",
    start_date=datetime.datetime(2021, 1, 1),
    description="A dag to load data from bronze to silver table",
    schedule=[bronze_table],
    catchup=False,
    tags=["ingestion"],
    default_args=DEFAULT_ARGS
) as dag:
    spark_config = ConfigLoader().get_spark_config()
    submit_job = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=str(app_path.resolve()),
        application_args=[
            "--SRC_TABLE", Variable.get("BRONZE_TABLE"),
            "--TARGET_TABLE", Variable.get("SILVER_TABLE")
        ],
        outlets=[silver_table],
        conf=spark_config,
        **SPARK_DRIVER_ONFIG

    )
