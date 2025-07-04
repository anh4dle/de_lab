CREATE TABLE iceberg.default.taxitable (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
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
	external_location = 's3://taxidata/',
    format = 'PARQUET'
);


