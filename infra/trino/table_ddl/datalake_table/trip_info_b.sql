--Create iceberg table for raw layer
DROP TABLE IF EXISTS iceberg.default.trip_info_b;
CREATE TABLE iceberg.default.trip_info_b (
    VendorID BIGINT,
    tpep_pickup_datetime TIMESTAMP(0),
    tpep_dropoff_datetime TIMESTAMP(0),
    passenger_count INT,
    trip_distance DOUBLE,
    RatecodeID INT,
    store_and_fwd_flag VARCHAR,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
batch_id VARCHAR,
updated_date TIMESTAMP(0),
ingested_date TIMESTAMP(0))
WITH (
	location = 's3a://lake/bronze/',
    format = 'PARQUET'
);