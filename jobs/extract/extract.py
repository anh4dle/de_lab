import asyncio
import aiohttp  # Use this instead of requests because it is non-blocking
from pathlib import Path
from utils.logger import logger
from utils.minio_utils import MinIOWrapper
from datetime import datetime
import pytz
import io
# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")


def log_failed(filename, error, csv_writer, download_or_upload):
    formatted_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    csv_writer.writerow([filename, error, formatted_time])
    print(f'Cannot {download_or_upload} file {filename} because: {error}')


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


async def download_file_as_stream(aiohttp_session, download_url, retry_time, csv_writer):
    last_dash = download_url.rfind("/")
    filename = download_url[last_dash + 1:]

    for attemp in range(retry_time + 1):
        try:
            # Download bytes and write to a file
            async with aiohttp_session.get(download_url, ssl=False) as res:
                if res.status == 200:
                    print(f"Writing stream of data to {filename}")
                    data = await write_to_bytesIO(res)
                    return data
        except Exception as e:
            log_failed(filename, str(e), csv_writer, 'download')
            print(f'Failed to download {filename}')
        if attemp < retry_time:
            await asyncio.sleep(2)
        else:
            print(f'Failed to download {filename} after all attempts')
            return


async def upload_to_minio(minio: MinIOWrapper, filename, dir_path, data, retry_time, csv_writer):
    for attemp in range(retry_time + 1):
        try:
            minio_upload_path = Path(dir_path) / filename
            minio.upload_stream_obj(bucket_name='lake',
                                    object_name=minio_upload_path, data=data)
        except Exception as e:
            log_failed(filename, str(e), csv_writer, 'upload')
            print(f'Failed to upload {filename}')
        if attemp < retry_time:
            await asyncio.sleep(2)
        else:
            print(f'Failed to upload {filename} after all attempts')
            return


async def download_and_upload(minio: MinIOWrapper, aiohttp_session, dir_path, download_url, retry_time, csv_writer):
    last_dash = download_url.rfind("/")
    filename = download_url[last_dash + 1:]
    data = await download_file_as_stream(aiohttp_session, download_url, retry_time, csv_writer)
    await upload_to_minio(minio, filename, dir_path, data, retry_time, csv_writer)


def convert_month_to_string(month):
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    return month


async def extract_data(minio: MinIOWrapper, current_year, end_year, csv_writer):
    urls = []
    retry_times = 0

    while current_year < end_year:
        minio_upload_path = str(Path('lake/parquetfiles') / str(current_year))
        month = 1
        downloaded_files = minio.get_downloaded_files_by_year(
            'lake', current_year)
        while month < 12:
            if month == 4:
                month_str = convert_month_to_string(month)
                download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}-{month_str}.parquet"
                last_dash = download_url.rfind("/")
                filename = download_url[last_dash + 1:]
                filepath = f"{str(current_year)}/{download_url[last_dash + 1:]}"
                if filepath not in downloaded_files:
                    print(f"Downloading {filename}")
                    urls.append((minio_upload_path, download_url))
            month += 1
        current_year += 1
    print(urls)

    async with aiohttp.ClientSession() as aiohttp_session:
        # Tach download and upload
        tasks = [download_and_upload(minio, aiohttp_session, dir_path, url, retry_times, csv_writer)
                 for dir_path, url in urls]

        await asyncio.gather(*tasks)
