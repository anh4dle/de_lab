--Create iceberg table for raw layer
DROP TABLE IF EXISTS iceberg.default.trip_info_s;
CREATE TABLE iceberg.default.trip_info_s (
    VendorID INT,
    pickup_datetime TIMESTAMP(0),
    dropoff_datetime TIMESTAMP(0),
    trip_distance DOUBLE,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    total_amount DOUBLE,
    trip_id VARCHAR,
batch_id VARCHAR,
updated_date TIMESTAMP(0),
ingested_date TIMESTAMP(0)
)
WITH (
	location = 's3a://lake/silver/'
);