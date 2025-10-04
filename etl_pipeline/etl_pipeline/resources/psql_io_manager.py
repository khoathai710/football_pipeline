from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
)
logs = logging.getLogger("psql_io_manager")

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass

    # TODO: your code here
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        table = context.asset_key.path[-1]
        schema = context.asset_key.path[0]
        logs.info(f"Writing to table {schema}.{table} in PostgreSQL")
        with connect_psql(self._config) as conn:
            obj.to_sql(table, con=conn,schema='gold', if_exists='append', index=False)