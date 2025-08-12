from datetime import datetime
import asyncio
import aiohttp  # Use this instead of requests because it is non-blocking
from pathlib import Path
from utils.logger import logger
from utils.minio_utils import MinIOWrapper
from datetime import datetime
import pytz
import io
from utils.logger import logger
from utils.trino_utils import get_trino_client, check_if_uploaded, log_status
# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")


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


async def download_file_as_stream(trino_conn, aiohttp_session, download_url, retry_time):
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
            logger.info(f'Failed to download {filename}')

            log_status(trino_conn, filename, download_url,
                       "failed", datetime.now(), str(e))
        if attemp < retry_time:
            await asyncio.sleep(2)
        else:
            logger.info(f'Failed to download {filename} after all attempts')

    return


async def upload_to_minio(trino_conn, minio: MinIOWrapper, bucket_name, download_url, dir_path, data, retry_time):
    last_dash = download_url.rfind("/")
    filename = download_url[last_dash + 1:]
    for attemp in range(retry_time + 1):
        try:
            # Only upload if not exist
            minio_upload_path = Path(dir_path) / filename
            minio.create_bucket(bucket_name)
            minio.upload_stream_obj(bucket_name=bucket_name,
                                    object_name=minio_upload_path, data=data)
            logger.info(f"Upload {filename} succeeded.")
            return True
        except Exception as e:
            logger.info(f'Failed to upload {filename}')
            log_status(trino_conn, filename, download_url,
                       "failed", datetime.now(), str(e))
        if attemp < retry_time:
            await asyncio.sleep(2)
        else:
            logger.info(f'Failed to upload {filename} after all attempts')

    return False


async def download_and_upload(trino_conn, minio: MinIOWrapper, bucket_name, aiohttp_session, dir_path, download_url, retry_time):
    data = await download_file_as_stream(trino_conn, aiohttp_session, download_url, retry_time)
    await upload_to_minio(trino_conn, minio, bucket_name,  download_url, dir_path, data, retry_time)


def convert_month_to_string(month):
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    return month


async def submit_download_and_upload(minio_url, minio_access, minio_pass, bucket_name, current_year, end_year):
    urls = []
    retry_times = 0
    minio = MinIOWrapper(minio_url, minio_access, minio_pass)
    trino_conn = get_trino_client()

    while current_year <= end_year:
        minio_upload_path = str(Path('parquetfiles') / str(current_year))
        month = 1
        downloaded_files = minio.get_downloaded_files_by_year(
            bucket_name, current_year)
        logger.info(f"Logging downloaded files {downloaded_files}")
        while month < 12:
            month_str = convert_month_to_string(month)
            download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}-{month_str}.parquet"
            urls.append((minio_upload_path, download_url))
            month += 1
        current_year += 1
    async with aiohttp.ClientSession() as aiohttp_session:
        last_dash = download_url.rfind("/")
        filename = download_url[last_dash + 1:]
        if not check_if_uploaded(trino_conn, filename):
            # Wrap coroutines in tasks
            tasks = [download_and_upload(trino_conn, minio, bucket_name, aiohttp_session, dir_path, url, retry_times)
                     for dir_path, url in urls]
            # Schedule and execute all tasks
            await asyncio.gather(*tasks)
        else:
            logger.info(filename + "file already uploaded")
