import os
import asyncio
import csv
import asyncio
import pytz
from utils.minio_utils import MinIOWrapper
from jobs.extract.download_and_upload import extract_data
from utils.spark_wrapper import SparkWrapper
from utils.config_loader import ConfigLoader
from utils.logger import logger
from pyspark.sql.functions import sha2, concat_ws

# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")


def etl_gold(sparkWrapper, SRC_TABLE, TARGET_TABLE):
    spark = sparkWrapper.spark

    try:
        df_source = spark.read.table(SRC_TABLE)

        query = """
        select trip_id as id, year(dropoff_datetime) as year, month(dropoff_datetime) as month, 
        day(dropoff_datetime) as day, sum(total_amount) as total_revenue
        from iceberg.default.trip_info
        group by year(dropoff_datetime), month(dropoff_datetime), day(dropoff_datetime), trip_id
        LIMIT 10
        """
        df_source = spark.sql(query)

        df_source.show(10)
        df_source.createOrReplaceTempView('SOURCE_TABLE')

        update_cols = ', '.join(
            f"t.{col} = s.{col}" for col in df_source.columns
        )

        SQL = f"""
        MERGE INTO {TARGET_TABLE} t
        USING SOURCE_TABLE s
        ON t.id = s.id
        WHEN MATCHED THEN
        UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(SQL)
    except Exception as e:
        logger.error("Printing exception err:" + str(e))
        # print("Printing exception err:" + str(e))
    spark.stop()


def etl_silver(sparkWrapper, SRC_TABLE, TARGET_TABLE):
    spark = sparkWrapper.spark

    try:
        df_source = spark.read.table(SRC_TABLE)
        df_source = df_source.withColumnRenamed("tpep_pickup_datetime",
                                                "pickup_datetime").withColumnRenamed("tpep_dropoff_datetime",
                                                                                     "dropoff_datetime")

        columns_to_keep = ["VendorID",
                           "pickup_datetime",
                           "dropoff_datetime",
                           "trip_distance",
                           "PULocationID",
                           "DOLocationID",
                           "payment_type",
                           "total_amount"]

        df_source = df_source.select(columns_to_keep)

        df_source = df_source.withColumn(
            "trip_id",
            sha2(concat_ws("||",
                           "VendorID",
                           "pickup_datetime",
                           "dropoff_datetime",
                           "PULocationID",
                           "DOLocationID",
                           "payment_type",
                           "total_amount",
                           ), 256)
        )
        df_source.show(10)
        df_source.createOrReplaceTempView('SOURCE_TABLE')

        update_cols = ', '.join(
            f"t.{col} = s.{col}" for col in df_source.columns
        )

        SQL = f"""
        MERGE INTO {TARGET_TABLE} t
        USING SOURCE_TABLE s
        ON t.trip_id = s.trip_id
        WHEN MATCHED THEN
        UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(SQL)
    except Exception as e:
        # logger.error("Printing exception err:" + str(e))
        print("Printing exception err:" + str(e))
    spark.stop()


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


async def main():
    # await test_extract()
    # test_spark()
    APP_NAME = 'spark_app'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'
    spark_config_path = os.getenv('LOCAL_SPARK_CONFIG_PATH')
    config = ConfigLoader(spark_config_path)
    spark_config_dict = config.get_yaml_config_dict()

    sparkWrapper = SparkWrapper(
        APP_NAME, spark_config_dict, CATALOG_NAME, DB_NAME)

    SRC_TABLE = 'default.trip_info'
    TARGET_TABLE = 'default.trip_info_g'
    # etl_silver(sparkWrapper, SRC_TABLE, TARGET_TABLE)
    etl_gold(sparkWrapper, SRC_TABLE, TARGET_TABLE)

if __name__ == "__main__":
    asyncio.run(main())
