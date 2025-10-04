# IO Managers
from .resources.mysql_io_manager import MySQLIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from .resources.spark_io_manager import SparkIOManager

# Configs
from config.config import MYSQL_CONFIG,MINIO_CONFIG,PSQL_CONFIG,SPARK_CONFIG

# Assets
from etl_pipeline import assets 
from dagster import Definitions, load_assets_from_modules

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources = {
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
        "spark_io_manager": SparkIOManager(MINIO_CONFIG),
    
    }
)
