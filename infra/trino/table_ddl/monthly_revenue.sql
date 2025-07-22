--Create iceberg table for raw layer
DROP TABLE IF EXISTS iceberg.default.monthly_revenue;
CREATE TABLE iceberg.default.monthly_revenue (
    VendorID INT,
    pickup_datetime TIMESTAMP(0),
    dropoff_datetime TIMESTAMP(0),
    trip_distance DOUBLE,
    RatecodeID INT,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    total_amount DOUBLE
)
WITH (
	location = 's3a://lake/silver/monthly_revenue',
    format = 'PARQUET'
);