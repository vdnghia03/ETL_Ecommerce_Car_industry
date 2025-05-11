
# import os
# from dagster import Definitions
# from .assets.bronze_layer import *
# from .assets.silver_layer import *
# from .assets.gold_layer import *
# from .assets.warehouse_layer import *
# from .resources.dbt_resource import dbt_resource, dbt_assets, DbtCliResource
# from .resources.minio_io_manager import MinIOIOManager
# from .resources.mysql_io_manager import MySQLIOManager
# from .resources.psql_io_manager import PostgreSQLIOManager

# MYSQL_CONFIG = {
#     "host": os.getenv("MYSQL_HOST"),
#     "port": os.getenv("MYSQL_PORT"),
#     "database": os.getenv("MYSQL_DATABASE"),
#     "user": os.getenv("MYSQL_USER"),
#     "password": os.getenv("MYSQL_PASSWORD")
# }
# MINIO_CONFIG = {
#     "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#     "bucket": os.getenv("DATALAKE_BUCKET"),
#     "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
#     "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
# }
# PSQL_CONFIG = {
#     "host": os.getenv("POSTGRES_HOST"),
#     "port": os.getenv("POSTGRES_PORT"),
#     "database": os.getenv("POSTGRES_DB"),
#     "user": os.getenv("POSTGRES_USER"),
#     "password": os.getenv("POSTGRES_PASSWORD")
# }



# ls_asset=[asset_factory(table) for table in tables] + [silver_crm_cust_info
#                                                        , silver_crm_prd_info
#                                                        , silver_crm_sales_details
#                                                        , silver_erp_cust_az12
#                                                        , silver_erp_loc_a101
#                                                        , silver_erp_px_cat_g1v2
#                                                        , gold_dim_customer
#                                                        , gold_dim_product
#                                                        , gold_fact_sales
#                                                        , warehouse_dim_product
#                                                        , warehouse_dim_customer
#                                                        , warehouse_fact_sales ]


# # Định nghĩa dbt assets
# @dbt_assets(manifest="/opt/dagster/app/etl_pipeline/dbt/warehouse_tests/target/manifest.json")
# def dbt_warehouse_tests(context, dbt: DbtCliResource):
#     yield from dbt.cli(["run"], context=context).stream()
#     yield from dbt.cli(["test"], context=context).stream()

# defs = Definitions(
#     assets=ls_asset,
#     resources={
#         "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
#         "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
#         "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
#         "dbt": dbt_resource
#     }
# )

import os
from dagster import Definitions
# from dagster_dbt import dbt_assets
from dagster_dbt import DbtCliResource, dbt_assets
from .assets.bronze_layer import *
from .assets.silver_layer import *
from .assets.gold_layer import *
from .assets.warehouse_layer import *
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from .resources.dbt_resource import dbt_resource

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD")
}
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
}
PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

ls_asset = [asset_factory(table) for table in tables] + [
    silver_crm_cust_info,
    silver_crm_prd_info,
    silver_crm_sales_details,
    silver_erp_cust_az12,
    silver_erp_loc_a101,
    silver_erp_px_cat_g1v2,
    gold_dim_customer,
    gold_dim_product,
    gold_fact_sales,
    warehouse_dim_customer,
    warehouse_dim_product,
    warehouse_fact_sales
]

@dbt_assets(manifest="/opt/dagster/app/etl_pipeline/dbt/warehouse_tests/target/manifest.json")
def dbt_warehouse_tests(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()

defs = Definitions(
    assets=ls_asset + [dbt_warehouse_tests],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
        "dbt": dbt_resource
    }
)

