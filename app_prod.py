import os
import asyncio
import csv
import asyncio
# from utils.minio_utils import MinIOWrapper
# from jobs.extract.download_files import extract_data
from spark_utils.spark_wrapper import SparkWrapper
from config.config_loader import ConfigLoader
from utils.logger import logger
from pyspark.sql.functions import sha2, concat_ws

# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
# tz = pytz.timezone("Asia/Ho_Chi_Minh")


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


async def main():
    APP_NAME = 'spark_app'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'

    spark_config_path = os.getenv('PROD_SPARK_CONFIG_PATH')
    config = ConfigLoader(spark_config_path)
    # config = ConfigLoader("/opt/bitnami/spark/spark_config_prod.yaml")
    spark_config_dict = config.get_yaml_config_dict()

    sparkWrapper = SparkWrapper(
        APP_NAME, spark_config_dict, CATALOG_NAME, DB_NAME)

    SRC_TABLE = 'default.trip_info'
    TARGET_TABLE = 'default.trip_info_g'
    etl_gold(sparkWrapper, SRC_TABLE, TARGET_TABLE)

if __name__ == "__main__":
    asyncio.run(main())
