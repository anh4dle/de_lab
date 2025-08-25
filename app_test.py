import pyarrow.parquet as pq
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, StringType, TimestampType, IntegerType
)
from pyspark.sql import SparkSession

desired_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", IntegerType(), True)
])

file_path = "yellow_tripdata_2025-05.parquet"  # or local path
# file_path = "fixed.parquet"  # or local path

# If s3, configure fs via s3fs
table = pq.read_table(file_path)
spark = SparkSession.builder \
    .appName("LocalParquetTest") \
    .master("local[*]") \
    .getOrCreate()
# df = spark.read.schema(desired_schema).parquet(file_path)
df = spark.read.parquet(file_path)
df.printSchema()

# print(table.schema)  # Arrow schema (may normalize types)

# # Raw parquet schema
# pf = pq.ParquetFile(file_path)
# # To inspect Parquet metadata structure:
# print(pf.schema)
