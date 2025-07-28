from utils.minio_utils import MinIOWrapper

#   upload_file_to_minio(minio_client, bucket_name,
#                                 object_name, file_path, df_upload_log)


def upload_file_to_minio(minio_client, bucket_name,
                         object_name, file_path, df_upload_log):

    MinIOWrapper.upload_stream_obj(bucket_name, object_name, data)
