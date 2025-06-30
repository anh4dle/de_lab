import asyncio 
import aiohttp #Use this instead of requests because it is non-blocking
from pathlib import Path
from utils.logger import logger
from datetime import datetime
import pytz
from collections import defaultdict
# base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
tz = pytz.timezone("Asia/Ho_Chi_Minh")

def log_failed_download(filename, error, csv_writer):    
    formatted_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    csv_writer.writerow([filename, error, formatted_time])
    logger.error(f'Cannot download file {filename} because: {error}')
    
def check_download_complete(total_bytes, res, filename):
    content_length = res.headers.get("Content-Length")
    if content_length and total_bytes != int(content_length):
        return False
    logger.info(f"Finished download {filename}")
    return True
        
async def download(session, dir_path, download_url, retry_time, error_file):
    last_dash = download_url.rfind("/")
    filename = download_url[last_dash + 1:]
    for attemp in range(retry_time + 1):
        try:
            #Download bytes and write to a file
            async with session.get(download_url, ssl=False) as res:
                if res.status == 200:
                    logger.info(f"Writing data to {filename}")
                    file_path = Path(dir_path) / filename
                    with open(file_path, "wb") as f:
                        total_bytes = 0
                        async for chunk in res.content.iter_chunked(1024):
                            f.write(chunk)
                            total_bytes += len(chunk)
                        if check_download_complete(total_bytes, res, filename):
                            return
                        else:
                            log_failed_download(filename, 'Incomplete download for file', error_file)
                    file_path.unlink()
        except Exception as e:
            log_failed_download(filename, str(e), error_file)
        if attemp < retry_time:
            await asyncio.sleep(2)
        else:
            logger.error(f'Failed to download {filename} after all attempts')
            return

def convert_month_to_string(month):
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    return month

async def extract(current_year, end_year, csv_writer, downloaded_files):
    urls = []
    retry_times = 0
    while current_year < end_year:
        dir_path = Path("data") / str(current_year)
        if not Path(dir_path).exists():
            Path(dir_path).mkdir(parents=True)
        month = 1
        while month < 12:
            month_str = convert_month_to_string(month)
            download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}-{month_str}.parquet"
            last_dash = download_url.rfind("/")
            filename = download_url[last_dash + 1:]

            if filename not in downloaded_files:
                print(f"Downloading {filename} {download_url}")
                urls.append((dir_path , download_url))
            month += 1
        current_year += 1
    
    async with aiohttp.ClientSession() as session:
        tasks = [download(session, dir_path, url, retry_times, csv_writer) for dir_path, url in urls]
        await asyncio.gather(*tasks)

