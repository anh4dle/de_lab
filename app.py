import os
import asyncio
import csv
import asyncio
import pytz
from utils.minio_utils import MinIOWrapper
from jobs.extract.download_files import extract_data
from spark.spark_wrapper import SparkWrapper
from config.config_loader import ConfigLoader
# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")


def test_spark():
    APP_NAME = 'spark_app'
    OBJECT_PATH = 's3a://lake/raw/2020/'

    config_path = os.getenv('LOCAL_CONFIG_PATH')
    config = ConfigLoader(config_path)
    config_dict = config.get_yaml_config_dict()
    spark = SparkWrapper(APP_NAME, config_dict)
    spark.read_parquet(OBJECT_PATH)

    spark.stop()


async def test_extract():
    minio_url, minio_access, minio_pass = os.getenv("MINIO_URL_LOCAL"), os.getenv(
        "MINIO_ROOT_USER"), os.getenv("MINIO_ROOT_PASSWORD")
    minio = MinIOWrapper(minio_url, minio_access, minio_pass)
    start_year, end_year = 2020, 2021
    with open('logs/failed_download.csv', 'a') as file:
        csv_logger = csv.writer(file)
        if file.tell() == 0:
            csv_logger.writerow(['filename', 'error', 'timestamp'])
    await extract_data(minio, start_year, end_year, csv_logger)


async def main():
    # await test_extract()
    test_spark()


if __name__ == "__main__":
    asyncio.run(main())
