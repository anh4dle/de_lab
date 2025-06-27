
from jobs.extract.extract import extract
import asyncio

def main():
    current_year = 2020
    end_year = 2021
    asyncio.run(extract(current_year, end_year))

main()