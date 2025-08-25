from pyspark.sql.functions import col


from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, StringType, TimestampType, IntegerType
)
from utils.config_loader import ConfigLoader
from utils.logger import logger

import argparse
from utils.spark_wrapper import SparkWrapper
desired_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])


def main(SPARK_CONFIG_PATH):
    APP_NAME = 'standardize'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'

    config = ConfigLoader(SPARK_CONFIG_PATH)
    spark_config_dict = config.get_yaml_config_dict()

    spark = SparkWrapper(APP_NAME, spark_config_dict,
                         CATALOG_NAME, DB_NAME).spark
    df = spark.read.option("mergeSchema", "true").schema(desired_schema).parquet(
        "s3a://lake/parquetfiles/2025/yellow_tripdata_2025-05.parquet")

#   df_source = spark_session.read.option("mergeSchema", "true").schema(
    # desired_schema).parquet(OBJECT_PATH)
    df = df.withColumn("VendorID", col("VendorID").cast(LongType()))
    # Write with desired schema enforced
    df.coalesce(1).write.mode("overwrite").parquet(
        "s3a://lake/parquetfiles_fixed/2025/yellow_tripdata_2025-05.parquet")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark_config_path", required=True)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    import sys
    try:
        main(
            args.spark_config_path,
        )
    except Exception as e:

        logger.error(f"Error running job {e}")
        sys.exit(1)
