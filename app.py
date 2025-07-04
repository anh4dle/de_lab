import os
import asyncio
import csv
import io
import asyncio
import aiohttp  # Use this instead of requests because it is non-blocking
from pathlib import Path

from datetime import datetime
import pytz
from utils.minio_utils import get_minio_client, upload_stream_obj, get_downloaded_files_by_year

# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")


def log_failed_download(filename, error, csv_writer):
    formatted_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    csv_writer.writerow([filename, error, formatted_time])
    print(f'Cannot download file {filename} because: {error}')


def check_download_complete(total_bytes, res, filename):
    content_length = res.headers.get("Content-Length")
    if content_length and total_bytes != int(content_length):
        return False
    print(f"Finished download {filename}")
    return True


async def write_to_bytesIO(response):
    data = io.BytesIO()
    async for chunk in response.content.iter_chunked(1024):
        data.write(chunk)
    data.seek(0)
    return data


async def upload_to_minio(minio, minio_upload_path, data):
    try:
        upload_stream_obj(minio, bucket_name='lake',
                          object_name=minio_upload_path, data=data)
        print("Upload successful!")
    except Exception as e:
        raise Exception(f"Upload failed: {e}")


async def download_and_upload(minio, aiohttp_session, dir_path, download_url, retry_time, error_logs):
    last_dash = download_url.rfind("/")
    filename = download_url[last_dash + 1:]
    for attemp in range(retry_time + 1):
        try:
            # Download bytes and write to a file
            async with aiohttp_session.get(download_url, ssl=False) as res:
                if res.status == 200:
                    print(f"Writing data to {filename}")
                    data = await write_to_bytesIO(res)

                    minio_upload_path = Path(dir_path) / filename
                    upload_to_minio(minio, minio_upload_path, data)

        except Exception as e:
            log_failed_download(filename, str(e), error_logs)
        if attemp < retry_time:
            await asyncio.sleep(2)
        else:
            print(f'Failed to download {filename} after all attempts')
            return


def convert_month_to_string(month):
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    return month


async def extract(minio, current_year, end_year, error_logs):
    urls = []
    retry_times = 0

    while current_year < end_year:
        minio_upload_path = str(Path('lake/parquetfiles') / str(current_year))
        month = 1
        downloaded_files = get_downloaded_files_by_year(
            minio, 'lake', current_year)
        while month < 3:
            month_str = convert_month_to_string(month)
            download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}-{month_str}.parquet"
            last_dash = download_url.rfind("/")
            filename = download_url[last_dash + 1:]
            filepath = f"{str(current_year)} / {download_url[last_dash + 1:]}"
            if filepath not in downloaded_files:
                print(f"Downloading {filename}")
                urls.append((minio_upload_path, download_url))
            month += 1
        current_year += 1

    async with aiohttp.ClientSession() as aiohttp_session:
        tasks = [download_and_upload(minio, aiohttp_session, dir_path, url, retry_times, error_logs)
                 for dir_path, url in urls]
        await asyncio.gather(*tasks)


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
        asyncio.run(extract(minio, current_year, end_year,
                    error_logs))


main()
