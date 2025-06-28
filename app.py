
from jobs.extract.extract import extract
import asyncio
from pathlib import Path
import csv

def main():
    file_path = 'logs/failed_download.csv'
    if not Path('logs').exists():
        Path('logs').mkdir(parents=True)
    
    downloaded_files = { f.name for f in Path('data').rglob('*') if f.is_file() }
    
    with open(file_path, 'a') as f:
        csv_writer = csv.writer(f)
        if Path(file_path).stat().st_size == 0:
            header = ['File', 'Error', 'Time']
            csv_writer.writerow(header)
        current_year = 2020
        end_year = 2021
        asyncio.run(extract(current_year, end_year, csv_writer, downloaded_files))

main()