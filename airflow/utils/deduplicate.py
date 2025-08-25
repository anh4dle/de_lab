from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_wrapper import SparkWrapper
import yaml


def deduplicate():
    app_name = 'deduplicate'
    catalog_name = 'iceberg'
    db_name = 'default'
    table = 'log_table'
    with open("airflow/utils/spark_config_dev.yaml", "r") as f:
        config_dict = yaml.safe_load(f)
        sparkWrapper = SparkWrapper(
            app_name, config_dict, catalog_name, db_name)
        spark = sparkWrapper.spark
        df = spark.read.format("iceberg").load(
            f"{catalog_name}.{db_name}.{table}")

        # Deduplicate
        w = Window.partitionBy("filename").orderBy(
            F.col("updated_date").desc())
        df_dedup = df.withColumn(
            "rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        # Overwrite table
        df_dedup.writeTo(
            f"{catalog_name}.{db_name}.{table}").overwritePartitions()


deduplicate()
