import asyncio
import aiohttp  # Use this instead of requests because it is non-blocking
from pathlib import Path
from utils.logger import logger
from utils.minio_utils import MinIOWrapper
from datetime import datetime
import pytz
import io
from utils.logger import logger
# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")


def log_failed(filename, error, csv_logger, download_or_upload):
    formatted_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    csv_logger.writerow([filename, error, formatted_time])
    logger.info(
        f'Cannot {download_or_upload} file {filename} because: {error}')


def check_download_complete(total_bytes, res, filename):
    content_length = res.headers.get("Content-Length")
    if content_length and total_bytes != int(content_length):
        return False
    logger.info(f"Finished download {filename}")
    return True


async def write_to_bytesIO(response):
    data = io.BytesIO()
    async for chunk in response.content.iter_chunked(1024):
        data.write(chunk)
    data.seek(0)
    return data


async def download_file_as_stream(aiohttp_session, download_url, retry_time, csv_logger):
    last_dash = download_url.rfind("/")
    filename = download_url[last_dash + 1:]

    for attemp in range(retry_time + 1):
        try:
            # Download bytes and write to a file
            async with aiohttp_session.get(download_url, ssl=False) as res:
                if res.status == 200:
                    logger.info(f"Writing stream of data to {filename}")
                    data = await write_to_bytesIO(res)
                    return data
        except Exception as e:
            log_failed(filename, str(e), csv_logger, 'download')
            logger.info(f'Failed to download {filename}')
        if attemp < retry_time:
            await asyncio.sleep(2)
        else:
            logger.info(f'Failed to download {filename} after all attempts')
            return


async def upload_to_minio(minio: MinIOWrapper, bucket_name, download_url, dir_path, data, retry_time, csv_logger):
    last_dash = download_url.rfind("/")
    filename = download_url[last_dash + 1:]
    for attemp in range(retry_time + 1):
        try:
            # Only upload if not exist
            minio_upload_path = Path(dir_path) / filename
            minio.create_bucket(bucket_name)
            minio.upload_stream_obj(bucket_name=bucket_name,
                                    object_name=minio_upload_path, data=data)
            #Log
            return
        except Exception as e:
            log_failed(filename, str(e), csv_logger, 'upload')
            logger.info(f'Failed to upload {filename}')
        if attemp < retry_time:
            await asyncio.sleep(2)
        else:
            logger.info(f'Failed to upload {filename} after all attempts')
            return


async def download_and_upload(minio: MinIOWrapper, bucket_name, aiohttp_session, dir_path, download_url, retry_time, csv_logger):
    data = await download_file_as_stream(aiohttp_session, download_url, retry_time, csv_logger)
    await upload_to_minio(minio, bucket_name,  download_url, dir_path, data, retry_time, csv_logger)
    # Log


def convert_month_to_string(month):
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    return month


async def submit_download_and_upload(minio: MinIOWrapper, bucket_name, current_year, end_year, csv_logger):
    urls = []
    retry_times = 0

    while current_year < end_year:
        minio_upload_path = str(Path('parquetfiles') / str(current_year))
        month = 1
        downloaded_files = minio.get_downloaded_files_by_year(
            bucket_name, current_year)
        logger.info(f"Logging downloaded files {downloaded_files}")
        while month < 12:
            if month == 4:
                month_str = convert_month_to_string(month)
                download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}-{month_str}.parquet"
                last_dash = download_url.rfind("/")
                filename = download_url[last_dash + 1:]
                filepath = f"parquetfiles/{str(current_year)}/{download_url[last_dash + 1:]}"
                if filepath not in downloaded_files:
                    logger.info(f"Downloading {filename}")
                    urls.append((minio_upload_path, download_url))
            month += 1
        current_year += 1
    async with aiohttp.ClientSession() as aiohttp_session:
        # Wrap coroutines in tasks
        # Download if filename not existed
        tasks = [download_and_upload(minio, bucket_name, aiohttp_session, dir_path, url, retry_times, csv_logger)
                 for dir_path, url in urls]
        # Schedule and execute all tasks
        await asyncio.gather(*tasks)
