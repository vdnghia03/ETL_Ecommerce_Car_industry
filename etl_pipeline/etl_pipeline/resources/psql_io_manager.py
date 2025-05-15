# from contextlib import contextmanager
# from datetime import datetime
# import pandas as pd
# from sqlalchemy import create_engine
# from dagster import IOManager, io_manager, InputContext, OutputContext


# @contextmanager
# def connect_psql(config):
#     conn_info = (
#     f"postgresql+psycopg2://{config['user']}:{config['password']}" + f"@{config['host']}:{config['port']}" + f"/{config['database']}"
#     )
#     db_conn = create_engine(conn_info)
#     try:
#         yield db_conn
#     except Exception:
#         raise

# class PostgreSQLIOManager(IOManager):
#     def __init__(self, config):
#         self._config = config
#     def load_input(self, context: InputContext) -> pd.DataFrame:
#         pass
#     def handle_output(self, context: OutputContext, obj: pd.DataFrame):
#         schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
#         with connect_psql(self._config) as conn:
#         # insert new data
#             ls_columns = (context.metadata or {}).get("columns", [])
#             obj[ls_columns].to_sql(
#                 name=f"{table}",
#                 con=conn,
#                 schema=schema,
#                 if_exists="replace",
#                 index=False,
#                 chunksize=10000,
#                 method="multi"
#             )

# from contextlib import contextmanager
# import pandas as pd
# from sqlalchemy import create_engine
# from dagster import IOManager, InputContext, OutputContext
# import logging

# # Cấu hình logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# @contextmanager
# def connect_psql(config):
#     """Context manager to handle PostgreSQL connection."""
#     conn_info = (
#         f"postgresql+psycopg2://{config['user']}:{config['password']}" +
#         f"@{config['host']}:{config['port']}" +
#         f"/{config['database']}"
#     )
#     db_conn = create_engine(conn_info, pool_size=5, max_overflow=10)
#     try:
#         yield db_conn
#     except Exception as e:
#         logger.error(f"Database connection failed: {str(e)}")
#         raise
#     finally:
#         db_conn.dispose()

# class PostgreSQLIOManager(IOManager):
#     def __init__(self, config):
#         self._config = config

#     def load_input(self, context: InputContext) -> pd.DataFrame:
#         """Load data from PostgreSQL as a DataFrame."""
#         schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
#         query = f"SELECT * FROM {schema}.{table}"
#         with connect_psql(self._config) as conn:
#             try:
#                 df = pd.read_sql(query, conn)
#                 logger.info(f"Loaded data from {schema}.{table} successfully.")
#                 return df
#             except Exception as e:
#                 logger.error(f"Failed to load data from {schema}.{table}: {str(e)}")
#                 raise

#     def handle_output(self, context: OutputContext, obj: pd.DataFrame):
#         """Write DataFrame to PostgreSQL."""
#         schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
#         if_exists = context.metadata.get("if_exists", "replace")  # Default to 'replace'

#         with connect_psql(self._config) as conn:
#             try:
#                 obj.to_sql(
#                     name=table,
#                     con=conn,
#                     schema=schema,
#                     if_exists=if_exists,
#                     index=False,
#                     chunksize=10000,
#                     method="multi"
#                 )
#                 logger.info(f"Wrote data to {schema}.{table} successfully with {len(obj)} records.")
#             except Exception as e:
#                 logger.error(f"Failed to write data to {schema}.{table}: {str(e)}")
#                 raise

from contextlib import contextmanager
import pandas as pd
from sqlalchemy import create_engine, text  # Thêm import text
from dagster import IOManager, InputContext, OutputContext
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@contextmanager
def connect_psql(config):
    """Context manager to handle PostgreSQL connection."""
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}" +
        f"@{config['host']}:{config['port']}" +
        f"/{config['database']}"
    )
    db_conn = create_engine(conn_info, pool_size=5, max_overflow=10)
    try:
        yield db_conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise
    finally:
        db_conn.dispose()

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load data from PostgreSQL as a DataFrame."""
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        query = f"SELECT * FROM {schema}.{table}"
        with connect_psql(self._config) as conn:
            try:
                df = pd.read_sql(query, conn)
                logger.info(f"Loaded data from {schema}.{table} successfully.")
                return df
            except Exception as e:
                logger.error(f"Failed to load data from {schema}.{table}: {str(e)}")
                raise

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Write DataFrame to PostgreSQL."""
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]

        with connect_psql(self._config) as conn:
            try:
                # Lấy Connection từ Engine để thực thi lệnh DELETE
                with conn.connect() as connection:
                    delete_query = f"DELETE FROM {schema}.{table}"
                    # Chuyển chuỗi SQL thành đối tượng thực thi bằng text()
                    connection.execute(text(delete_query))
                    # Commit để đảm bảo lệnh DELETE được áp dụng
                    connection.commit()
                    logger.info(f"Deleted all existing data from {schema}.{table} before inserting new data.")

                # Insert dữ liệu mới vào bảng
                obj.to_sql(
                    name=table,
                    con=conn,
                    schema=schema,
                    if_exists="append",  # Không drop bảng, chỉ append
                    index=False,
                    chunksize=10000,
                    method="multi"
                )
                logger.info(f"Wrote data to {schema}.{table} successfully with {len(obj)} records.")
            except Exception as e:
                logger.error(f"Failed to write data to {schema}.{table}: {str(e)}")
                raise