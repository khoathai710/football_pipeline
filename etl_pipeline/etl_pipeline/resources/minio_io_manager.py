from contextlib import contextmanager
from minio import Minio
from dagster import IOManager, OutputContext, InputContext
from typing import Union
from datetime import datetime
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import OutputContext, InputContext
from tempfile import mktemp


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


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.client = Minio(
            config['endpoint'],
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            secure=config.get('secure', False)
        )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        
        # Sử dụng mktemp để tạo đường dẫn file tạm thời
        tmp_file_path = mktemp(suffix='.parquet')

        if context.has_asset_partitions:
            partition_key = context.partition_key
            # Nếu partition_key chỉ toàn số, thì thêm prefix 'week_'
            if partition_key.isdigit():
                partition_key = f"week_{partition_key}"
            partition_path = "/".join([key, partition_key])
            return f"{partition_path}.parquet", tmp_file_path
        else:
            return f"{key}.parquet", tmp_file_path


    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # Convert DataFrame to parquet format
        key_name, tmp_file_path = self._get_path(context)
        obj.to_parquet(tmp_file_path)
        try:
            # Upload file to MinIO
            bucket_name = self._config['bucket_name']
            self.client.fput_object(
                bucket_name=bucket_name,
                object_name=key_name,
                file_path=tmp_file_path,
                content_type='application/parquet'
            )
            # Clean up tmp file
            os.remove(tmp_file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to handle output: {e}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        try:
            # Download file from MinIO
            bucket_name = self._config['bucket_name']
            self.client.fget_object(
                bucket_name=bucket_name,
                object_name=key_name,
                file_path=tmp_file_path
            )
            # Load DataFrame from parquet file
            df = pd.read_parquet(tmp_file_path)
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load input: {e}")