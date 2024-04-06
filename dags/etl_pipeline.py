from airflow import DAG

from datetime import datetime

from airflow.operators.python import PythonOperator

# from mysql_io_manager import MySQLIOManager
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager

import os
import random

from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio


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

PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"), 
    "database": os.getenv("POSTGRES_DB"), 
    "user": os.getenv("POSTGRES_USER"), 
    "password": os.getenv("POSTGRES_PASSWORD"),
}


dag = DAG(
    dag_id='elt_pipeline',
    start_date= datetime(2024, 3, 19),
    schedule_interval=None
)

mysql_io_manager = MySQLIOManager(MYSQL_CONFIG)
minio_io_manager = MinIOIOManager(MINIO_CONFIG)
psql_io_manager = PostgreSQLIOManager(PSQL_CONFIG)

def bronze_olist_orders_dataset():
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