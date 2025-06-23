import asyncio 
import aiohttp #Use this instead of requests because it is non-blocking
import os

base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

async def download(session, download_url):
    async with session.get(download_url, ssl=False) as res:
        if res.status == 200:
            last_dash = download_url.rfind("/")
            filename = download_url[last_dash + 1:]
            print(f"Writing {filename}")
            with open(filename, "wb") as f:
                async for chunk in res.content.iter_chunked(1024):
                    f.write(chunk)
            
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
    while current_year < end_year:
        create_directory(str(current_year))
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

current_year = 2020
end_year = 2025
asyncio.run(main(current_year, end_year))