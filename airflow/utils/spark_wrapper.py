from pyspark.sql import SparkSession
import pyspark


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
        # v = config_dict['spark']['configs']['spark.jars']
        # builder = builder.config("spark.jars", v)

        return builder.getOrCreate()
    # TODO: Keep spark config dict or no?

    def print_config(self):
        for k, v in self.spark.sparkContext.getConf().getAll():
            print(f"Applying spark config: {k} = {v}")
