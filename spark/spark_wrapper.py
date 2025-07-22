from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import pyspark
# Load the env file
load_dotenv()
print('asd', os.path.dirname(pyspark.__file__))

os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME")
# os.environ["SPARK_HOME"] = os.getenv("SPARK_HOME")
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars jars/hadoop-aws-3.3.5.jar,jars/aws-java-sdk-bundle-1.12.261.jar,jars/iceberg-spark-runtime-3.4_2.12-1.5.2.jar pyspark-shell'


class SparkWrapper:

    def __init__(self, app_name, config_dict, catalog_name, db_name):
        self.spark = self.build_session(app_name, config_dict)
        self.spark.sparkContext.setLogLevel("DEBUG")
        self.spark.catalog.setCurrentCatalog(catalog_name)
        self.spark.catalog.setCurrentDatabase(db_name)

    def build_session(self, app_name, config_dict):
        builder = SparkSession.builder.appName(
            app_name)
        for k, v in config_dict['spark']['configs'].items():
            builder = builder.config(k, v)
        v = config_dict['spark']['configs']['spark.jars']
        builder = builder.config("spark.jars", v)

        return builder.getOrCreate()

    def print_config(self):
        for k, v in self.spark.sparkContext.getConf().getAll():
            print(f"Applying spark config: {k} = {v}")
