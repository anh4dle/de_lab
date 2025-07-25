## Data lake project

- Data pipeline requirements:

  - Scalability
  - Debugging: Use logging, monitoring, and alerting
  - Reliability: Idempotency
  - Fault Tolerance: Retry mechanisms
  - Document: architecture, data flow

- Data source:

  - Ingest yellow taxi data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
  - Metadata: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

- Domain requirements for Gold Zone:

  - Reporting question for Gold Zone:
    - Monthly Revenue Per Pickup Zone: total amount group by PULocationID and month
    - Trip Volume Trends by Zone and Time: (\*) group by zone and tpep_pickup_datetime(hourly) to schedule driver better
  - ML question for Gold Zone: - Forecast trip volume by zone and tpep_pickup_datetime(hourly) based on past volume

- Misc:
  - Start env on windows: source env/Scripts/activate

## Todo:

<<<<<<< HEAD
- Switch to Spark cluster
=======
- Fix to run on Spark
- Dynamic config
-
>>>>>>> bc276f3 (upgrade spark to 3.5.0)
- Data Quality Requirements
