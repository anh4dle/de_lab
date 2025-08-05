import pyarrow.parquet as pq

file_path = "yellow_tripdata_2020-04.parquet"  # or local path
# If s3, configure fs via s3fs
table = pq.read_table(file_path)

print(table.schema)  # Arrow schema (may normalize types)

# Raw parquet schema
pf = pq.ParquetFile(file_path)
meta = pf.schema_arrow
print(meta)

# To inspect Parquet metadata structure:
print(pf.schema)
