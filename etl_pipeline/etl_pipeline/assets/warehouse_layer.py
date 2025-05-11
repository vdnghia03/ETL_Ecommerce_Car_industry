import pandas as pd
from dagster import asset, Output, AssetIn
import logging

logger = logging.getLogger(__name__)

@asset(
    ins={
        "gold_dim_customer": AssetIn(
            key_prefix=["ERM", "gold"]
        )
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse"],
    metadata={
        "columns": [
            "customer_key",
            "customer_id",
            "customer_number",
            "first_name",
            "last_name",
            "country",
            "marital_status",
            "gender",
            "birthday",
            "create_date"
        ]
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer",
    description="Load gold_dim_customer into PostgreSQL warehouse schema as dim_customer"
)
def warehouse_dim_customer(gold_dim_customer: pd.DataFrame) -> Output[pd.DataFrame]:
    logger.info(f"Processing warehouse_dim_customer with {len(gold_dim_customer)} records.")
    df = gold_dim_customer.copy()

    # Enforce data types for PostgreSQL compatibility
    df = df.astype({
        'customer_key': 'int64',
        'customer_id': 'str',
        'customer_number': 'str',
        'first_name': 'str',
        'last_name': 'str',
        'country': 'str',
        'marital_status': 'str',
        'gender': 'str'
    })

    # Convert datetime columns and normalize to keep only date (YYYY-MM-DD)
    for col in ['birthday', 'create_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()

    # Drop dwh_create_date
    if 'dwh_create_date' in df.columns:
        df = df.drop(columns=['dwh_create_date'])

    # Prepare metadata
    metadata = {
        "schema": "warehouse",
        "table": "dim_customer",
        "records": len(df),
        "if_exists": "replace"
    }

    return Output(df, metadata=metadata)


# ==============================================================

@asset(
    ins={
        "gold_dim_product": AssetIn(
            key_prefix=["ERM", "gold"]
        )
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse"],
    metadata={
        "columns": [
            "product_key",
            "product_id",
            "product_number",
            "product_name",
            "category_id",
            "category",
            "subcategory",
            "maintenance",
            "cost",
            "product_line",
            "start_date"
        ]
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer",
    description="Load gold_dim_product into PostgreSQL warehouse schema as dim_product"
)
def warehouse_dim_product(gold_dim_product: pd.DataFrame) -> Output[pd.DataFrame]:
    logger.info(f"Processing warehouse_dim_product with {len(gold_dim_product)} records.")
    df = gold_dim_product.copy()

    # Enforce data types for PostgreSQL compatibility
    df = df.astype({
        'product_key': 'int64',
        'product_id': 'str',
        'product_number': 'str',
        'product_name': 'str',
        'category_id': 'str',
        'category': 'str',
        'subcategory': 'str',
        'maintenance': 'str',
        'cost': 'float64',
        'product_line': 'str'
    })

    # Convert datetime columns and normalize to keep only date (YYYY-MM-DD)
    for col in ['start_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()

    # Drop dwh_create_date
    if 'dwh_create_date' in df.columns:
        df = df.drop(columns=['dwh_create_date'])

    # Prepare metadata
    metadata = {
        "schema": "warehouse",
        "table": "dim_product",
        "records": len(df),
        "if_exists": "replace"
    }

    return Output(df, metadata=metadata)

# ==============================================================

@asset(
    ins={
        "gold_fact_sales": AssetIn(
            key_prefix=["ERM", "gold"]
        )
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse"],
    metadata={
        "columns": [
            "order_number",
            "product_key",
            "customer_key",
            "order_date",
            "ship_date",
            "due_date",
            "sales_amount",
            "quantity",
            "price"
        ]
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer",
    description="Load gold_fact_sales into PostgreSQL warehouse schema as fact_sales"
)
def warehouse_fact_sales(gold_fact_sales: pd.DataFrame) -> Output[pd.DataFrame]:
    logger.info(f"Processing warehouse_fact_sales with {len(gold_fact_sales)} records.")
    df = gold_fact_sales.copy()

    # Enforce data types for PostgreSQL compatibility
    df = df.astype({
        'order_number': 'str',
        'product_key': 'int64',
        'customer_key': 'int64',
        'sales_amount': 'float64',
        'quantity': 'int64',
        'price': 'float64'
    })

    # Convert datetime columns and normalize to keep only date (YYYY-MM-DD)
    for col in ['order_date', 'ship_date', 'due_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()

    # Drop dwh_create_date
    if 'dwh_create_date' in df.columns:
        df = df.drop(columns=['dwh_create_date'])

    # Prepare metadata
    metadata = {
        "schema": "warehouse",
        "table": "fact_sales",
        "records": len(df),
        "if_exists": "replace"
    }

    return Output(df, metadata=metadata)