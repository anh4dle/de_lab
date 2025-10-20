from pyspark.sql.functions import col
import asyncio
import asyncio
import pytz
from utils.spark_wrapper import SparkWrapper
from utils.logger import logger
import argparse
from pyspark.sql.types import LongType

from utils.minio_utils import MinIOWrapper

tz = pytz.timezone("Asia/Ho_Chi_Minh")


def read_parquet(spark_session, OBJECT_PATH):
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


async def main(year, TARGET_TABLE,  MINIO_URL, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD):

    APP_NAME = 'parquet_to_bronze'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'
    minioWrapper = MinIOWrapper(
        MINIO_URL, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)

    sparkWrapper = SparkWrapper(
        APP_NAME, CATALOG_NAME, DB_NAME)

    file_names = minioWrapper.get_downloaded_files_by_year("lake", year)

    for file_name in file_names:

        df = read_parquet(sparkWrapper.spark, f"s3a://lake/{file_name}")
        etl_source_to_bronze(sparkWrapper.spark, df, TARGET_TABLE)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--SRC_TABLE", required=True)
    parser.add_argument("--TARGET_TABLE", required=True)
    parser.add_argument("--MINIO_URL", required=True)
    parser.add_argument("--MINIO_ROOT_USER", required=True)
    parser.add_argument("--MINIO_ROOT_PASSWORD", required=True)

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    import sys
    try:
        asyncio.run(main(
            args.SRC_TABLE,
            args.TARGET_TABLE,
            args.MINIO_URL,
            args.MINIO_ROOT_USER,
            args.MINIO_ROOT_PASSWORD
        ))
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
