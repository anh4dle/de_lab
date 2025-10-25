DROP TABLE IF EXISTS iceberg.default.trip_info_g;
CREATE TABLE iceberg.default.trip_info_g (
	id VARCHAR,
    year INT,
    month INT,
    day INT,
    total_revenue DOUBLE,
batch_id VARCHAR,
updated_date TIMESTAMP(0),
ingested_date TIMESTAMP(0)
)
WITH (
    location = 's3a://lake/gold/',
    format_version = 2,
    partitioning = ARRAY['year', 'month', 'day']
);