import os


class ConfigLoader:
    def __init__(self):
        self.HIVE_URL = os.getenv("HIVE_URL")
        self.MINIO_URL = os.getenv("MINIO_URL")
        self.MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
        self.MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

    def get_spark_config(self):
        return {
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.sql.parquet.enableVectorizedReader': "false",
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.type': "hive",
            'spark.sql.catalog.iceberg.uri': self.HIVE_URL,
            'spark.hadoop.fs.s3a.endpoint': self.MINIO_URL,
            'spark.hadoop.fs.s3a.access.key': self.MINIO_ROOT_USER,
            'spark.hadoop.fs.s3a.secret.key': self.MINIO_ROOT_PASSWORD
        }
