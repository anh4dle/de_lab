import os
import asyncio
import csv
import asyncio
from pathlib import Path
from datetime import datetime
import pytz
from utils.minio_utils import get_minio_client
from jobs.extract import extract_data
# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")


def main():
    file_path = 'logs/failed_download.csv'
    if not Path('logs').exists():
        Path('logs').mkdir(parents=True)

    # Get downloaded files

    minio_access, minio_pass = os.getenv(
        'MINIO_ROOT_USER'), os.getenv('MINIO_ROOT_PASSWORD')
    minio = get_minio_client(minio_access, minio_pass)
    with open(file_path, 'a') as f:
        error_logs = csv.writer(f)
        if Path(file_path).stat().st_size == 0:
            header = ['File', 'Error', 'Time']
            error_logs.writerow(header)
        current_year = 2020
        end_year = 2022
        asyncio.run(extract_data(minio, current_year, end_year,
                    error_logs))


main()
