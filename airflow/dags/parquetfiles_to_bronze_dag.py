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


with DAG(
    dag_id="parquetfiles_to_bronze",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion"],
    default_args=DEFAULT_ARGS,
) as dag:
    spark_home = Path(Variable.get("SPARK_HOME"))
    app_path = spark_home / "jobs" / "load" / "parquetfiles_to_bronze.py"

    # Cannot use inside taskflow API so we use context manager
    submit_job = SparkSubmitOperator(
        task_id="parquet_to_bronze",
        conn_id="spark_conn",
        application=str(app_path.resolve()),
        application_args=[
            Variable.get("PARQUETFILES_PATH"),
            Variable.get("BRONZE_TABLE"),
            Variable.get("SPARK_CONFIG_PATH"),
        ],
    )

"""
Input: sparksubmitOperator(app, args)
APP Input: configDict, source and target table, sparkWrapper.
SparkWrapper Input:(app name, dict, catalog, db)
Create sparkWrapper in dag or inside app. Can do inside app since dict is tight coupling already. Fine for now. so we can create sparkWrapper in the app
mean that we only need to pass source and target table
"""
