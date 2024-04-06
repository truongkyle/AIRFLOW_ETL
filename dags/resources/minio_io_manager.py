import os
from contextlib import contextmanager 
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"), 
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False, 
        )
    try:
        yield client
    except Exception:
        raise

class MinIOIOManager():
    def __init__(self, config):
        self._config = config

    def _get_path(self, context):
        layer =  context["layer"]
        schema = context["schema"]
        table = context["table"]
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"), "-".join([layer, schema, table])
        )
        print("-----------tmp_file_path-----------:", tmp_file_path)
        
        return f"{key}.pq", tmp_file_path
    
    def handle_output(self, context, obj):
        key_name, tmp_file_path = self._get_path(context)
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, tmp_file_path)

        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")
                client.fput_object(bucket_name, key_name, tmp_file_path)
                row_count = len(obj)
                os.remove(tmp_file_path)
                return {
                    "path": key_name,
                    "tmp": tmp_file_path
                }
                          
        except Exception:
            raise 
    
    def load_input(self, context):
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)
        try:
            with connect_minio(self._config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")
                client.fget_object(bucket_name, key_name, tmp_file_path)
                pd_data = pd.read_parquet(tmp_file_path)
                return pd_data
        except Exception:
            raise

# @contextmanager
# def connect_minio(config):
#     client = Minio(
#         endpoint=config.get("endpoint_url"), 
#         access_key=config.get("aws_access_key_id"),
#         secret_key=config.get("aws_secret_access_key"),
#         secure=False, 
#         )
#     try:
#         yield client
#     except Exception:
#         raise

# class MinIOIOManager():
#     def __init__(self, config):
#         self._config = config

#     def _get_path(self, context):
#         layer =  context["layer"]
#         schema = context["schema"]
#         table = context["table"]
#         key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
#         tmp_file_path = "tmp/file-{}-{}.parquet".format(
#             datetime.today().strftime("%Y%m%d%H%M%S"), "-".join([layer, schema, table])
#         )
        
#         return f"{key}.pq", tmp_file_path
    
#     def handle_output(self, context, obj):
#         key_name, tmp_file_path = self._get_path(context)
#         table = pa.Table.from_pandas(obj)
#         pq.write_table(table, tmp_file_path)

#         try:
#             bucket_name = self._config.get("bucket")
#             with connect_minio(self._config) as client:
#                 found = client.bucket_exists(bucket_name)
#                 if not found:
#                     client.make_bucket(bucket_name)
#                 else:
#                     print(f"Bucket {bucket_name} already exists")
#                 client.fput_object(bucket_name, key_name, tmp_file_path)
#                 row_count = len(obj)
#                 os.remove(tmp_file_path)
#                 return {
#                     "path": key_name,
#                     "tmp": tmp_file_path
#                 }
                          
#         except Exception:
#             raise 
    
#     def load_input(self, context):
#         bucket_name = self._config.get("bucket")
#         key_name, tmp_file_path = self._get_path(context)
#         try:
#             with connect_minio(self._config) as client:
#                 found = client.bucket_exists(bucket_name)
#                 if not found:
#                     client.make_bucket(bucket_name)
#                 else:
#                     print(f"Bucket {bucket_name} already exists")
#                 client.fget_object(bucket_name, key_name, tmp_file_path)
#                 pd_data = pd.read_parquet(tmp_file_path)
#                 return pd_data
#         except Exception:
#             raise