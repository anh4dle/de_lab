import requests
import asyncio
import aiohttp
import os

base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

async def download(session, download_url):
    async with session.get(download_url) as res:
        if res.status == 200:
            first_dash = download_url.rfind("/")
            filename = download_url[first_dash + 1:]
            print(filename)

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

async def main():
    year = 2020
    urls = []
    while year < 2025:
        create_directory(str(year))
        month = 1
        while month < 13:
            month_str = month_to_string(month)
            base_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month_str}.parquet"
            urls.append(base_url)
            month += 1
        year += 1
        break
    async with aiohttp.ClientSession() as session:
        tasks = [download(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

        for result in results:
            print(result)

asyncio.run(main())