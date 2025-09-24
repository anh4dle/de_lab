from pyspark.sql import SparkSession
import pyspark


class SparkWrapper:
    def __init__(self, app_name,  catalog_name, db_name):
        self.spark = SparkSession.builder.appName(
            app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel("DEBUG")
        self.spark.catalog.setCurrentCatalog(catalog_name)
        self.spark.catalog.setCurrentDatabase(db_name)

    def print_config(self):
        for k, v in self.spark.sparkContext.getConf().getAll():
            print(f"Applying spark config: {k} = {v}")
