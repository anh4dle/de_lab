import datetime
from airflow.decorators import dag, task
import os
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils.spark_wrapper import SparkWrapper
from config.config_loader import ConfigLoader


@dag(
    dag_id="parquetfiles_to_bronze",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
)
def task_flow():
    SPARK_HOME = Variable.get("SPARK_HOME")
    PARQUETFILES_PATH = Variable.get("OBJECT_PATH")
    BRONZE_TABLE = Variable.get("BRONZE_TABLE")

    @task
    def load_data_to_bronze():
        APP_NAME = 'spark_app'
        CATALOG_NAME = 'iceberg'
        DB_NAME = 'default'

    # Input dict to spark submit operator
        spark_config_path = Variable.get('SPARK_CONFIG_PATH')
        config = ConfigLoader(spark_config_path)
        spark_config_dict = config.get_yaml_config_dict()

        sparkWrapper = SparkWrapper(
            APP_NAME, spark_config_dict, CATALOG_NAME, DB_NAME)

        # app_prod take in table name and use existing config from spark

        spark = sparkWrapper.spark
        submit_job = SparkSubmitOperator(
            application="${SPARK_HOME}/app_prod.py", task_id="submit_job")
    load_data_to_bronze()


dag = task_flow()
