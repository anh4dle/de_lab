import datetime
from airflow.decorators import dag, task
import os
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils.spark_wrapper import SparkWrapper
from utils.config_loader import ConfigLoader


@dag(
    dag_id="parquetfiles_to_bronze",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
)
def task_flow():
    PARQUETFILES_PATH = Variable.get("PARQUETFILES_PATH")
    BRONZE_TABLE = Variable.get("BRONZE_TABLE")

    @task
    def load_data_to_bronze():
        APP_NAME = 'spark_app'
        CATALOG_NAME = 'iceberg'
        DB_NAME = 'default'

        # Input dict to spark submit operator
        config = ConfigLoader(os.environ.get("SPARK_CONFIG_PATH"))
        spark_config_dict = config.get_yaml_config_dict()

        app_path = os.path.join(os.environ.get(
            "SPARK_HOME", "/opt/spark"), "source_to_bronze.py")
        submit_job = SparkSubmitOperator(task_id="parquet_to_bronze", conn_id="spark_conn",
                                         application=app_path,
                                         application_args=[
                                             PARQUETFILES_PATH, BRONZE_TABLE])

    load_data_to_bronze()


"""
Input: sparksubmitOperator(app, args)
APP Input: configDict, source and target table, sparkWrapper.
SparkWrapper Input:(app name, dict, catalog, db)
Create sparkWrapper in dag or inside app. Can do inside app since dict is tight coupling already. Fine for now. so we can create sparkWrapper in the app
mean that we only need to pass source and target table
"""

dag = task_flow()
