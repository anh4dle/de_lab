from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

# Load the env file
load_dotenv()

os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME")
os.environ["SPARK_HOME"] = os.getenv("SPARK_HOME")


class SparkWrapper:

    def __init__(self, app_name, config_dict):
        self.spark = self.build_session(app_name, config_dict)

    def build_session(self, app_name, config_dict):
        builder = SparkSession.builder.appName(
            app_name)
        for k, v in config_dict['spark']['configs'].items():
            builder = builder.config(k, v)
        return builder.getOrCreate()

    def read_parquet(self, object_path):
        df = self.spark.read.parquet(object_path)
        df.show(10)
        return df
