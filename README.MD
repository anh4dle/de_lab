- Start env: source env/Scripts/activate
- Data lake project
- Ingest yellow taxi data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Hive metastore is like a dictionary we use to look up iceberg table which store in minio, trino is the query engine.
- ETL workflow: Source → Iceberg Raw Table → Spark Job → Iceberg Staging Table

## Todo:

- Test spark
- Create iceberg table for all bucket first
- Use spark to append data from source to iceberg
