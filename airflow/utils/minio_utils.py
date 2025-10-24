from minio import Minio
from utils.logger import logger
# from logger import logger


class MinIOWrapper:
    def __init__(self, minio_url, minio_access: str, minio_pass: str):
        self.minio_client = Minio(
            minio_url,
            access_key=minio_access,
            secret_key=minio_pass,
            secure=False,
        )

    """
    Return all downloaded monthly data file for this year
    """

    def get_downloaded_files_by_year(self, bucket, year):
        try:

            prefix = f"parquetfiles/{year}/"

            objects = self.minio_client.list_objects(
                bucket, prefix=prefix, recursive=True)
            result = [object.object_name for object in objects]
            return result
        except Exception as e:
            raise Exception(
                f"Failed to list objects at this year {year} because {e}")

    def get_year_dirs(self, bucket):
        try:
            # Ensure it's a string with a trailing slash
            prefix = f"parquetfiles/"

            objects = self.minio_client.list_objects(
                bucket, prefix=prefix)
            result = [object.object_name for object in objects]
            return result
        except Exception as e:
            raise Exception(
                f"Failed to list objects at this {bucket} because {e}")

    def create_bucket(self, bucket_name: str):
        if not self.minio_client.bucket_exists(bucket_name):
            self.minio_client.make_bucket(bucket_name)

    def upload_stream_obj(self, bucket_name: str, object_name: str, data):
        from pathlib import Path
        try:
            if isinstance(object_name, Path):
                object_name = str(object_name)
            if data:
                self.minio_client.put_object(bucket_name, object_name,
                                             data, length=data.getbuffer().nbytes)
                logger.info(f"Upload {object_name} succeeded.")
        except Exception as e:
            raise Exception(
                f"Upload stream obj {object_name} to bucket {bucket_name} failed {str(e)}")


# minio_url, minio_access, minio_pass = "localhost:9000", "minioadmin", "minioadmin"
# minio_client = MinIOWrapper(minio_url, minio_access, minio_pass)

# result = minio_client.get_downloaded_files_by_year("lake", 2025)
# print(result)
