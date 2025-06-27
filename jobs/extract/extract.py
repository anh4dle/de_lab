import asyncio 
import aiohttp #Use this instead of requests because it is non-blocking
import os
from utils.logger import logger

base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

def check_download_complete(total_bytes, res, filename):
    content_length = res.headers.get("Content-Length")
    raise Exception
    if content_length and total_bytes != int(content_length):
        logger.info(f"Incomplete download for file {filename}, expected {content_length}, got {total_bytes}")
        return False
    logger.info(f"Finished download {filename}")
    return True

def write_error_log(filename):
    with open('log.txt', "w") as f:
        f.write(f'Failed to download this file {filename}')
        
async def download(session, dir_path, download_url, retry_time):
    last_dash = download_url.rfind("/")
    filename = download_url[last_dash + 1:]
    for attemp in range(retry_time + 1):
        try:
            async with session.get(download_url, ssl=False) as res:
                if res.status == 200:
                    logger.info(f"Writing data to {filename}")
                    with open(os.path.join(dir_path, filename), "wb") as f:
                        total_bytes = 0
                        async for chunk in res.content.iter_chunked(1024):
                            f.write(chunk)
                            total_bytes += len(chunk)
                        if check_download_complete(total_bytes, res, filename):
                            return
        except Exception as e:
            logger.info(f'Cannot download file because of {e}')
        if attemp < retry_time:
            await asyncio.sleep(2)
            logger.info(f'Retrying')
        else:
            logger.error(f'Failed to download {filename}')
            return

#Make dir to store downloaded data, make dir based on year
def create_directory(dir_name):
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

def month_to_string(month):
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    return month

async def main(current_year, end_year):
    urls = []
    retry_times = 2
    while current_year < end_year:
        dir_path = os.path.join("data", str(current_year))
        create_directory(dir_path)
        month = 1
        while month < 3:
            month_str = month_to_string(month)
            base_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}-{month_str}.parquet"
            urls.append((dir_path , base_url))
            month += 1
        current_year += 1
    
    async with aiohttp.ClientSession() as session:
        tasks = [download(session, dir_path, url, retry_times) for dir_path, url in urls]
        await asyncio.gather(*tasks)

current_year = 2020
end_year = 2021
asyncio.run(main(current_year, end_year))