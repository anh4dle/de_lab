--Create iceberg table for raw layer
DROP TABLE IF EXISTS iceberg.default.taxi_raw;
CREATE TABLE iceberg.default.taxi_raw (
    VendorID INT,
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
    airport_fee DOUBLE
)
WITH (
	location = 's3a://lake/raw/',
    format = 'PARQUET'
);