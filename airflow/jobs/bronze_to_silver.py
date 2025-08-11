import asyncio
import argparse
import asyncio
from utils.spark_wrapper import SparkWrapper
from utils.config_loader import ConfigLoader
from utils.logger import logger
from pyspark.sql.functions import sha2, concat_ws

# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"


def bronze_to_silver(spark, SRC_TABLE, TARGET_TABLE):
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

        update_cols_stmt = ', '.join(
            f"t.{col} = s.{col}" for col in df_source.columns
        )

        SQL = f"""
        MERGE INTO {TARGET_TABLE} t
        USING SOURCE_TABLE s
        ON t.trip_id = s.trip_id
        WHEN MATCHED THEN
        UPDATE SET {update_cols_stmt}
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(SQL)
    except Exception as e:
        logger.error(f"logging exception err: {str(e)}")
    spark.stop()


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument("--SRC_TABLE", required=True)
    parser.add_argument("--TARGET_TABLE", required=True)
    parser.add_argument("--spark_config_path", required=True)
    args = parser.parse_args()
    return args


async def main(SRC_TABLE, TARGET_TABLE, SPARK_CONFIG_PATH):
    APP_NAME = 'bronze_to_silver'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'

    config = ConfigLoader(SPARK_CONFIG_PATH)
    spark_config_dict = config.get_yaml_config_dict()
    sparkWrapper = SparkWrapper(
        APP_NAME, spark_config_dict, CATALOG_NAME, DB_NAME)

    bronze_to_silver(sparkWrapper.spark, SRC_TABLE, TARGET_TABLE)


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
