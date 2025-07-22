--Create iceberg table for raw layer
DROP TABLE IF EXISTS iceberg.default.trip_volume;
CREATE TABLE iceberg.default.trip_volume (
    VendorID INT,
    pickup_datetime TIMESTAMP(0),
    dropoff_datetime TIMESTAMP(0),
    trip_distance DOUBLE,
    PULocationID INT,
    DOLocationID INT,
trip_id VARCHAR
)
WITH (
	location = 's3a://lake/silver/trip_volume',
    format = 'PARQUET'
);