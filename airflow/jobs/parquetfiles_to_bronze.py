from pyspark.sql.functions import col
import asyncio
import asyncio
import pytz
from utils.spark_wrapper import SparkWrapper
from utils.config_loader import ConfigLoader
from utils.logger import logger
import argparse
from pyspark.sql.types import *
from utils.minio_utils import MinIOWrapper

tz = pytz.timezone("Asia/Ho_Chi_Minh")
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


def read_parquet(spark_session, OBJECT_PATH):
    # List files, read, cast, union
    try:
        df_source = spark_session.read.parquet(OBJECT_PATH).withColumn(
            "VendorID", col("VendorID").cast(LongType()))
        df_source = df_source.drop(
            df_source.airport_fee)
        df_source.createOrReplaceTempView('SOURCE_TABLE')
        return df_source
    except Exception as e:
        logger.error(f"Error reading parquet file {e}")


def etl_source_to_bronze(spark_session, df_source, TARGET_TABLE):
    try:
        update_cols = ', '.join(
            f"t.{col} = s.{col}" for col in df_source.columns
        )
        logger.info(f"Showing parquet rows count {df_source.count()}")

        SQL = f"""
        MERGE INTO {TARGET_TABLE} t
        USING SOURCE_TABLE s
        ON t.tpep_pickup_datetime = s.tpep_pickup_datetime and t.tpep_dropoff_datetime = s.tpep_dropoff_datetime and t.total_amount = s.total_amount
        WHEN MATCHED THEN
        UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT *
        """
        spark_session.sql(SQL)
    except Exception as e:
        logger.error(f"logging exception err: {e}")
    spark_session.stop()


# Should have postgres client to insert log
async def main(year, TARGET_TABLE, SPARK_CONFIG_PATH):
    APP_NAME = 'parquet_to_bronze'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'

    MINIO_URL = "minio:9000"
    MINIO_ROOT_USER = "minioadmin"
    MINIO_ROOT_PASSWORD = "minioadmin"
    minio_client = MinIOWrapper(
        MINIO_URL, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)
    config = ConfigLoader(SPARK_CONFIG_PATH)
    spark_config_dict = config.get_yaml_config_dict()
    sparkWrapper = SparkWrapper(
        APP_NAME, spark_config_dict, CATALOG_NAME, DB_NAME)
    # Leave MinIO here and read file by file
    file_names = minio_client.get_downloaded_files_by_year("lake", year)

    for file_name in file_names:

        df = read_parquet(sparkWrapper.spark, f"s3a://lake/{file_name}")
        etl_source_to_bronze(sparkWrapper.spark, df, TARGET_TABLE)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--SRC_TABLE", required=True)
    parser.add_argument("--TARGET_TABLE", required=True)
    parser.add_argument("--spark_config_path", required=True)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    import sys
    try:
        asyncio.run(main(
            args.SRC_TABLE,
            args.TARGET_TABLE,
            args.spark_config_path,
        ))
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
