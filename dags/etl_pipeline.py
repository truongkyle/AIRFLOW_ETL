from airflow import DAG

from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

# from mysql_io_manager import MySQLIOManager
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager

from assets.bronze_layer import BronzeLayer
from assets.silver_layer import SilverLayer
from assets.gold_layer import GoldLayer

import os

import pandas as pd


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

mysql_io_manager = MySQLIOManager(MYSQL_CONFIG)
minio_io_manager = MinIOIOManager(MINIO_CONFIG)
psql_io_manager = PostgreSQLIOManager(PSQL_CONFIG)

bronze_layer = BronzeLayer(mysql_io_manager, minio_io_manager)
silver_layer = SilverLayer(minio_io_manager)
gold_layer = GoldLayer(minio_io_manager)

ls_tables = [
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "product_category_name_translation",
]

@dag(
    schedule_interval = None,
    start_date = datetime(2024, 4,7),
    tags = ["data_pipeline_stores"]
)
def data_piple_dag():

    @task
    def bronze_get_df_functions(dataset_name):
        return {"a":bronze_layer.get_data_from_mysql(dataset_name)}
    
    @task
    def bronze_transform_load_functions(df, dataset_name):
        context = {
                "layer": "bronze",
                "schema": "ecom",
                "table": dataset_name
            }
        bronze_layer.transform_and_load_to_minio(df, context)
        return context

    @task 
    def bronze_function(dataset_name):
        df = bronze_layer.get_data_from_mysql(dataset_name)
        context = {
                "layer": "bronze",
                "schema": "ecom",
                "table": dataset_name
            }
        bronze_layer.transform_and_load_to_minio(df, context)
        return context
    bronze_context_list = {}
    for dataset_name in ls_tables:
        bronze_context = bronze_function(dataset_name)
        bronze_context_list[f"{dataset_name}"] = bronze_context

    @task
    def silver_dim_functions(olist_products_dataset_context, product_category_name_translation_context):
        olist_products_dataset = minio_io_manager.load_input(olist_products_dataset_context)
        product_category_name_translation = minio_io_manager.load_input(product_category_name_translation_context)
        context = {
                "layer": "silver",
                "schema": "ecom",
                "table": "dim_products"
            }
        silver_layer.dim_products(olist_products_dataset, product_category_name_translation, context)
        return context
    
    @task
    def silver_fact_sales_function(olist_orders_dataset_context, olist_order_items_dataset_context, olist_order_payments_dataset_context):
        bronze_olist_orders_dataset = minio_io_manager.load_input(olist_orders_dataset_context)
        bronze_olist_order_items_dataset = minio_io_manager.load_input(olist_order_items_dataset_context)
        bronze_olist_order_payments_dataset = minio_io_manager.load_input(olist_order_payments_dataset_context)
        context = {
                "layer": "silver",
                "schema": "ecom",
                "table": "fact_sales"
            }
        silver_layer.fact_sales(bronze_olist_orders_dataset, bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset, context)
        return context

    @task
    def gold_minio_function(fact_sales_context, dim_products_context):
        fact_sales = minio_io_manager.load_input(fact_sales_context)
        dim_products = minio_io_manager.load_input(dim_products_context)

        context = gold_layer.gold_sales_value_by_category(fact_sales, dim_products)

        return context
    
    @task
    def gold_psql_function(gold_sales_context):
        sales_values_df = minio_io_manager.load_input(gold_sales_context)
        context = {
            "schema": "gold",
            "table": "sales_values_by_category",
            "columns": sales_values_df.columns
        }
        psql_io_manager.handle_output(context, sales_values_df)


    silver_fact_sales_context = silver_fact_sales_function(bronze_context_list["olist_orders_dataset"],
                                                    bronze_context_list["olist_order_items_dataset"],
                                                    bronze_context_list["olist_order_payments_dataset"])
    
    silver_dim_products_context = silver_dim_functions(bronze_context_list["olist_products_dataset"], 
                                                bronze_context_list["product_category_name_translation"])
    
    gold_sales_context = gold_minio_function(silver_fact_sales_context, silver_dim_products_context)
    gold_psql_function(gold_sales_context)
    
data_pipeline_stores = data_piple_dag()