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


async def main(spark_config_path):
    APP_NAME = 'spark_app'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'

    # spark_config_path = os.getenv('LOCAL_SPARK_CONFIG_PATH')

    config = ConfigLoader(spark_config_path)
    spark_config_dict = config.get_yaml_config_dict()
    sparkWrapper = SparkWrapper(
        APP_NAME, spark_config_dict, CATALOG_NAME, DB_NAME)

    SRC_TABLE = 'default.trip_info'
    TARGET_TABLE = 'default.trip_info_g'
    etl_source_to_bronze(sparkWrapper, SRC_TABLE, TARGET_TABLE)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark_config_path", required=True)
    args = parser.parse_args()
    asyncio.run(main(args.spark_config_path))
