
from dagster_dbt import DbtCliResource
from dagster_dbt import dbt_assets

dbt_resource = DbtCliResource(
    project_dir="/opt/dagster/app/etl_pipeline/dbt/warehouse_tests",
    profiles_dir="/opt/dagster/app/etl_pipeline/dbt",
    target="dev"
)