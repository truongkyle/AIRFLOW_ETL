from airflow import DAG

from datetime import datetime

from airflow.operators.python import PythonOperator

# from .resources.minio_io_manager import MinioIOManager
# from .resources.mysql_io_manager import MySQLIOManager

import os
import random

from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

@contextmanager
def connect_mysql(config):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}" 
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info) 
    try:
        yield db_conn 
    except Exception:
        raise

class MySQLIOManager(): 
    def __init__(self, config):
        self._config = config

    def handle_output(self):
        pass

    def load_input(self):
        pass
    
    def extract_data(self, sql: str): 
        with connect_mysql(self._config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn) 
            return pd_data



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


MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"), 
    "database": os.getenv("MYSQL_DATABASE"), 
    "user": os.getenv("MYSQL_USER"), 
    "password": os.getenv("MYSQL_PASSWORD"),
}

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"), 
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
}

dag = DAG(
    dag_id='elt_pipeline',
    start_date= datetime(2024, 3, 19),
    schedule_interval=None
)

mysql_io_manager = MySQLIOManager(MYSQL_CONFIG)
minio_io_manager = MinIOIOManager(MINIO_CONFIG)

def bronze_olist_orders_dataset(ti):
    print(MYSQL_CONFIG)
    sql_stm = "SELECT * FROM olist_orders_dataset LIMIT 10"
    pd_data = mysql_io_manager.extract_data(sql_stm)
    tmp_file_path = '/tmp/testfile.csv'
    pd_data.to_csv(tmp_file_path, index=False)
    return {
        "tmp_file_path":tmp_file_path,
        "records count": len(pd_data),
    }
    # ti.xcom_push(key='bronze_dataset', value=pd_data)

def transform_and_load_to_minio(**metadata):
    data = metadata['task_instance'].xcom_pull(task_ids='mysql_get_data')
    context = {
        "layer": "bronze",
        "schema": "public",
        "table": "item_order"
    }
    df = pd.read_csv(data.get("tmp_file_path"))
    minio_io_manager.handle_output(context, df)

    return {
        "context": context,
    }
    
def get_data_from_minio():
    context = {
        "layer": "bronze",
        "schema": "public",
        "table": "item_order"
    }
    df = minio_io_manager.load_input(context)
    df.to_csv("/tmp/test_load_data.csv", index=False)

mysql_get_data = PythonOperator(
    task_id = "mysql_get_data",
    python_callable=bronze_olist_orders_dataset,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_and_load_to_minio,
    provide_context=True,  # Cho phép truyền context từ Airflow
    dag=dag
)

get_data = PythonOperator(
    task_id = "get_data_from_minio",
    python_callable= get_data_from_minio,
    dag=dag
)

mysql_get_data >> transform_task >> get_data