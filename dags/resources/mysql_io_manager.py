from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine

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

    def load_input(self) -> pd.DataFrame:
        pass
    
    def extract_data(self, sql: str) -> pd.DataFrame: 
        with connect_mysql(self._config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn) 
            return pd_data