import asyncio
from utils.spark_wrapper import SparkWrapper
from utils.config_loader import ConfigLoader
from utils.logger import logger
import argparse


def silver_to_gold(spark, SRC_TABLE, TARGET_TABLE):

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
        logger.error("logger.infoing exception err: " + str(e))

    spark.stop()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--SRC_TABLE", required=True)
    parser.add_argument("--TARGET_TABLE", required=True)
    parser.add_argument("--spark_config_path", required=True)
    args = parser.parse_args()
    return args


async def main(SRC_TABLE, TARGET_TABLE, SPARK_CONFIG_PATH):
    APP_NAME = 'silver_to_gold'
    CATALOG_NAME = 'iceberg'
    DB_NAME = 'default'

    config = ConfigLoader(SPARK_CONFIG_PATH)
    spark_config_dict = config.get_yaml_config_dict()
    sparkWrapper = SparkWrapper(
        APP_NAME, spark_config_dict, CATALOG_NAME, DB_NAME)

    silver_to_gold(sparkWrapper.spark, SRC_TABLE, TARGET_TABLE)


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
