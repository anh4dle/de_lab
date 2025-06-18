import requests
import aiohttp

main_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
async def download(session, url):
    async with session.get('http://httpbin.org/get') as res:
        print(res.status)
        print(await res.text())

def main():
    pass