import os


class ConfigLoader:
    def get_spark_config(self):
        HIVE_URL = os.getenv("HIVE_URL")
        MINIO_URL = os.getenv("MINIO_URL")
        MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
        MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
        return {
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.sql.parquet.enableVectorizedReader': "false",
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.type': "hive",
            'spark.sql.catalog.iceberg.uri': HIVE_URL,
            'spark.hadoop.fs.s3a.endpoint': MINIO_URL,
            'spark.hadoop.fs.s3a.access.key': MINIO_ROOT_USER,
            'spark.hadoop.fs.s3a.secret.key': MINIO_ROOT_PASSWORD
        }
