from contextlib import contextmanager
from datetime import datetime
import os
from typing import Union
from dagster import IOManager, io_manager, InputContext, OutputContext

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio


@contextmanager
def connect_minio(config):
    minio_client = Minio(
        endpoint=config["endpoint_url"],
        access_key=config["aws_access_key_id"],
        secret_key=config["aws_secret_access_key"],
        secure=False,
    )
    try:
        yield minio_client
    except Exception:
        raise

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join(context.asset_key.path)
        )
        
        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y%m%d%H%M%S"
            partition_str = start.strftime(dt_format) + "_" + end.strftime(dt_format)
            return os.path.join(key, f"{partition_str}.pq"), tmp_file_path
        else:
            return f"{key}.pq", tmp_file_path
        
    # def handle_output(self, context: OutputContext, obj: pd.DataFrame):
    #     # convert to parquet format
    #     key_name, tmp_file_path = self._get_path(context)
    #     table = pa.Table.from_pandas(obj)
    #     pq.write_table(table, tmp_file_path)

    #     # upload to MinIO
    #     try:
    #         bucket_name = self._config.get("bucket")
    #         with connect_minio(self._config) as client:
    #             # Make bucket if not exist.
    #             found = client.bucket_exists(bucket_name)
    #             if not found:
    #                 client.make_bucket(bucket_name)
    #             else:
    #                 print(f"Bucket {bucket_name} already exists")
    #             client.fput_object(bucket_name, key_name, tmp_file_path)
    #             row_count = len(obj)
    #             context.add_output_metadata({"path": key_name, "tmp": tmp_file_path})
                
    #             # clean up tmp file
    #             os.remove(tmp_file_path)
    #     except Exception:
    #         raise

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # convert all object columns to string to avoid pyarrow conversion issues
        for col in obj.columns:
            if obj[col].dtype == "object":
                obj[col] = obj[col].astype(str)

        # convert to parquet format
        key_name, tmp_file_path = self._get_path(context)
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, tmp_file_path)

        # upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")

                client.fput_object(bucket_name, key_name, tmp_file_path)

                context.add_output_metadata({"path": key_name, "tmp": tmp_file_path})
                
                os.remove(tmp_file_path)
        except Exception:
            raise



    def load_input(self, context: InputContext) -> pd.DataFrame:
        bucket_name = self._config.get("bucket") 
        key_name, tmp_file_path = self._get_path(context)
        try:
            with connect_minio(self._config) as client:
                #Make bucket if not exist
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exist")
                client.fget_object(bucket_name, key_name, tmp_file_path)
                pd_data = pd.read_parquet(tmp_file_path)
                return pd_data
        except Exception:
           raise