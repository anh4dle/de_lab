import asyncio
import asyncio
import pytz
from utils.spark_wrapper import SparkWrapper
from utils.config_loader import ConfigLoader
from utils.logger import logger
import argparse

tz = pytz.timezone("Asia/Ho_Chi_Minh")


def etl_source_to_bronze(sparkWrapper, OBJECT_PATH, TARGET_TABLE):
    spark = sparkWrapper.spark

    try:
        df_source = spark.read.parquet(OBJECT_PATH)
        df_source.createOrReplaceTempView('SOURCE_TABLE')
        update_cols = ', '.join(
            f"t.{col} = s.{col}" for col in df_source.columns
        )

        SQL = f"""
        MERGE INTO {TARGET_TABLE} t
        USING SOURCE_TABLE s
        ON t.pickup_datetime = s.pickup_datetime and t.dropoff_datetime = s.dropoff_datetime
        WHEN MATCHED THEN
        UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(SQL)
    except Exception as e:
        print("Printing exception err:", e)
    spark.stop()


async def main(SRC_TABLE, TARGET_TABLE, spark_config_path):
    APP_NAME = 'parquet_to_bronze'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'

    config = ConfigLoader(spark_config_path)
    spark_config_dict = config.get_yaml_config_dict()
    sparkWrapper = SparkWrapper(
        APP_NAME, spark_config_dict, CATALOG_NAME, DB_NAME)

    SRC_TABLE = 'default.trip_info'
    TARGET_TABLE = 'default.trip_info_g'
    etl_source_to_bronze(sparkWrapper, SRC_TABLE, TARGET_TABLE)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--SRC_TABLE", required=True)
    parser.add_argument("--TARGET_TABLE", required=True)
    parser.add_argument("--spark_config_path", required=True)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(
        args.spark_config_path,
        args.input_path,
        args.output_path
    ))
