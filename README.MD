- Start env: source env/Scripts/activate
- Data lake project
- Ingest yellow taxi data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Hive metastore is like a dictionary we use to look up iceberg table which store in minio, trino is the query engine.
- ETL workflow: Source → Iceberg Raw Table → Spark Job → Iceberg Staging Table

## Todo:

- Find ways to not append duplicate val to raw layer
  - Search google on how not to append duplicate value to an iceberg table using Spark.
  - Implement MERGE INTO
