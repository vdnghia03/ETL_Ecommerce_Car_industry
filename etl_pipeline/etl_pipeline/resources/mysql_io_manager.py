from contextlib import contextmanager
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from dagster import IOManager, io_manager, InputContext, OutputContext


@contextmanager
def connect_mysql(config):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    )

    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise


class MySQLIOManager(IOManager):
    def __init__(self, config):
        self.config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # table_name = context.get_asset_key().path[-1]
        # with connect_mysql(self.config) as conn:
        #     obj.to_sql(table_name, conn, if_exists="replace", index=False)
        pass

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # table_name = context.get_asset_key().path[-1]
        # with connect_mysql(self.config) as conn:
        #     return pd.read_sql_table(table_name, conn)
        pass

    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self.config) as conn:
            pd_data = pd.read_sql_query(sql, conn)
            return pd_data