import os
import asyncio
import csv
import asyncio
import pytz
from utils.minio_utils import MinIOWrapper
from jobs.extract.extract import extract_data
from spark.spark_wrapper import SparkWrapper
from config.config_loader import ConfigLoader
# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")


def test_spark():
    APP_NAME = 'spark_app'
    config_path = os.getenv('LOCAL_CONFIG_PATH')
    config = ConfigLoader(config_path)
    config_dict = config.get_yaml_config_dict()
    spark = SparkWrapper(APP_NAME, config_dict).spark

    conf = spark.sparkContext.getConf().getAll()
    # Print key-value pairs
    for key, value in conf:
        print(f"Printing config val: {key} = {value}")


async def test_extract():
    minio_url, minio_access, minio_pass = os.getenv("MINIO_URL_LOCAL"), os.getenv(
        "MINIO_ROOT_USER"), os.getenv("MINIO_ROOT_PASSWORD")
    print(minio_url, minio_access, minio_pass)
    minio = MinIOWrapper(minio_url, minio_access, minio_pass)
    current_year, end_year = 2020, 2021
    with open('logs/failed_download.csv', 'a') as file:
        csv_writer = csv.writer(file)
        if file.tell() == 0:
            csv_writer.writerow(['filename', 'error', 'timestamp'])
        await extract_data(minio, current_year, end_year, csv_writer)


async def main():
    # test_spark()
    await test_extract()


if __name__ == "__main__":
    asyncio.run(main())
