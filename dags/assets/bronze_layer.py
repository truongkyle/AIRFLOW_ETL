from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager


import os
import pandas as pd

class BronzeLayer:
    def __init__(self, mysql_io_manager, minio_io_manager):
        self.mysql_io_manager = mysql_io_manager
        self.minio_io_manager = minio_io_manager

    def get_data_from_mysql(self, dataset_name):
        sql_stm = f"SELECT * FROM {dataset_name}"
        pd_data = self.mysql_io_manager.extract_data(sql_stm)
        tmp_file_path = f'/tmp/bronze_{dataset_name}.csv'
        pd_data.to_csv(tmp_file_path, index=False)
        return pd_data
    
    def transform_and_load_to_minio(self, object, context):
        # context = {
        #     "layer": "bronze",
        #     "schema": "public",
        #     "table": "item_order"
        # }
        self.minio_io_manager.handle_output(context, object)