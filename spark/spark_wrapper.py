from pyspark.sql import SparkSession
import yaml
import os
from dotenv import load_dotenv
# Load the env file
load_dotenv()

os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME")


class SparkWrapper:

    def __init__(self, app_name):

        self.spark = self.build_session(app_name)
        self.app_name = app_name

    def build_session(self, app_name):
        return SparkSession.builder.appName(app_name).getOrCreate()

    def read_parquet(self):
        df = self.spark.read.parquet("file:///home/user/data/taxi.parquet")

    def print_yaml(self):
        config_path = os.getenv('LOCAL_CONFIG_PATH')
        # CONFIG_PATH = load_dotenv('config/spark_config.yaml')
        if config_path is None:
            raise ValueError(
                "LOCAL_CONFIG_PATH is not set in the environment.")

        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
            print(data)


def main():
    spark = SparkWrapper("spark app")
    spark.print_yaml()


if __name__ == "__main__":
    main()
