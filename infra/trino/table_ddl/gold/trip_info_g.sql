DROP TABLE IF EXISTS iceberg.default.trip_info_g;
CREATE TABLE iceberg.default.trip_info_g (
	id VARCHAR,
    year INT,
    month INT,
    day INT,
    total_revenue DOUBLE
)
WITH (
    location = 's3a://lake/gold/trip_info_g',
    format_version = 2,
    partitioning = ARRAY['year', 'month', 'day']
);