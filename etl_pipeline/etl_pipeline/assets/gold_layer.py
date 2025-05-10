import pandas as pd
from dagster import asset, Output, AssetIn


# ==============================================================
# Dim Customer
# ==============================================================


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "silver_crm_cust_info": AssetIn(key_prefix=["ERM", "silver"]),
        "silver_erp_cust_az12": AssetIn(key_prefix=["ERM", "silver"]),
        "silver_erp_loc_a101": AssetIn(key_prefix=["ERM", "silver"])
    },
    key_prefix=["ERM", "gold"],
    description='Create dimension table gold_dim_customer by integrating data from Silver layer',
    group_name="Gold_layer",
    compute_kind="Pandas"
)
def gold_dim_customer(silver_crm_cust_info: pd.DataFrame, silver_erp_cust_az12: pd.DataFrame, silver_erp_loc_a101: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create copies of input DataFrames to avoid modifying originals
    ci = silver_crm_cust_info.copy()
    ca = silver_erp_cust_az12.copy()
    la = silver_erp_loc_a101.copy()

    # Merge DataFrames
    merged_df = ci.merge(ca, left_on='cst_key', right_on='cid', how='left', suffixes=('', '_erp_cust'))
    merged_df = merged_df.merge(la, left_on='cst_key', right_on='cid', how='left', suffixes=('', '_loc'))

    # Create customer_key (surrogate key)
    merged_df['customer_key'] = merged_df['cst_id'].rank(method='dense').astype('int64') - 1

    # Transform columns, include 'gen' temporarily for gender logic
    result = merged_df[[
        'customer_key',
        'cst_id',
        'cst_key',
        'cst_firstname',
        'cst_lastname',
        'cntry',
        'cst_marital_status',
        'cst_gndr',
        'gen',  # Include gen for processing
        'bdate',
        'cst_create_date'
    ]].rename(columns={
        'cst_id': 'customer_id',
        'cst_key': 'customer_number',
        'cst_firstname': 'first_name',
        'cst_lastname': 'last_name',
        'cntry': 'country',
        'cst_marital_status': 'marital_status',
        'cst_gndr': 'gender',
        'bdate': 'birthday',
        'cst_create_date': 'create_date'
    })

    # Handle gender logic
    result['gender'] = result.apply(
        lambda row: row['gender'] if row['gender'] != 'n/a' else row['gen'] if pd.notna(row['gen']) else 'n/a',
        axis=1
    )

    # Select final columns, drop 'gen' as it's no longer needed
    result = result[[
        'customer_key',
        'customer_id',
        'customer_number',
        'first_name',
        'last_name',
        'country',
        'marital_status',
        'gender',
        'birthday',
        'create_date'
    ]].drop_duplicates()

    # Add customer_key to result
    result['customer_key'] = merged_df['customer_key']

    # Enforce data types
    result = result.astype({
        'customer_key': 'int64',
        'customer_id': 'object',
        'customer_number': 'object',
        'first_name': 'object',
        'last_name': 'object',
        'country': 'object',
        'marital_status': 'object',
        'gender': 'object'
    })

    # Convert datetime columns
    for col in ['birthday', 'create_date']:
        result[col] = pd.to_datetime(result[col], errors='coerce')

    # Add dwh_create_date with current timestamp
    result['dwh_create_date'] = pd.Timestamp.now()

    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "gold_dim_customer",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )
# ==============================================================
# Dim Product
# ==============================================================



@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "silver_crm_prd_info": AssetIn(key_prefix=["ERM", "silver"]),
        "silver_erp_px_cat_g1v2": AssetIn(key_prefix=["ERM", "silver"])
    },
    key_prefix=["ERM", "gold"],
    description='Create dimension table gold_dim_product by integrating data from Silver layer',
    group_name="Gold_layer",
    compute_kind="Pandas"
)
def gold_dim_product(silver_crm_prd_info: pd.DataFrame, silver_erp_px_cat_g1v2: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create copies of input DataFrames to avoid modifying originals
    pn = silver_crm_prd_info.copy()
    pc = silver_erp_px_cat_g1v2.copy()

    # Merge DataFrames
    merged_df = pn.merge(pc, left_on='cat_id', right_on='id', how='left', suffixes=('', '_cat'))

    # Filter out historical data (prd_end_dt IS NULL)
    merged_df = merged_df[merged_df['prd_end_dt'].isna()]

    # Create product_key (surrogate key) based on prd_start_dt and prd_key

    merged_df['prd_start_dt'] = pd.to_datetime(merged_df['prd_start_dt'], errors='coerce')

    merged_df['product_key'] = merged_df[['prd_start_dt', 'prd_key']].apply(
        lambda row: f"{row['prd_start_dt']}_{row['prd_key']}" if pd.notna(row['prd_start_dt']) and pd.notna(row['prd_key']) else pd.NA,
        axis=1
    ).rank(method='dense').astype('int64') - 1

    # Transform columns
    result = merged_df[[
        'product_key',
        'prd_id',
        'prd_key',
        'prd_nm',
        'cat_id',
        'cat',
        'subcat',
        'maintenance',
        'prd_cost',
        'prd_line',
        'prd_start_dt'
    ]].rename(columns={
        'prd_id': 'product_id',
        'prd_key': 'product_number',
        'prd_nm': 'product_name',
        'cat_id': 'category_id',
        'cat': 'category',
        'subcat': 'subcategory',
        'maintenance': 'maintenance',
        'prd_cost': 'cost',
        'prd_line': 'product_line',
        'prd_start_dt': 'start_date'
    })

    # Enforce data types
    result = result.astype({
        'product_key': 'int64',
        'product_id': 'object',
        'product_number': 'object',
        'product_name': 'object',
        'category_id': 'object',
        'category': 'object',
        'subcategory': 'object',
        'maintenance': 'object',
        'cost': 'float64',
        'product_line': 'object'
    })

    # Convert start_date to datetime
    result['start_date'] = pd.to_datetime(result['start_date'], errors='coerce')

    # Add dwh_create_date with current timestamp
    result['dwh_create_date'] = pd.Timestamp.now()

    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "gold_dim_product",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )

# ==============================================================
# Fact Sales
# ==============================================================



@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "silver_crm_sales_details": AssetIn(key_prefix=["ERM", "silver"]),
        "gold_dim_product": AssetIn(key_prefix=["ERM", "gold"]),
        "gold_dim_customer": AssetIn(key_prefix=["ERM", "gold"])
    },
    key_prefix=["ERM", "gold"],
    description='Create fact table gold_fact_sales by integrating data from Silver and Gold layer',
    group_name="Gold_layer",
    compute_kind="Pandas"
)
def gold_fact_sales(silver_crm_sales_details: pd.DataFrame, gold_dim_product: pd.DataFrame, gold_dim_customer: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create copies of input DataFrames to avoid modifying originals
    sd = silver_crm_sales_details.copy()
    pr = gold_dim_product.copy()
    cu = gold_dim_customer.copy()

    # Merge DataFrames
    # Convert type of sls_prd_key and sls_cust_id to string for merging
    sd['sls_prd_key'] = sd['sls_prd_key'].astype(str)
    sd['sls_cust_id'] = sd['sls_cust_id'].astype(str)
    pr['product_number'] = pr['product_number'].astype(str)
    cu['customer_id'] = cu['customer_id'].astype(str)

    merged_df = sd.merge(pr, left_on='sls_prd_key', right_on='product_number', how='left', suffixes=('', '_prd'))
    merged_df = merged_df.merge(cu, left_on='sls_cust_id', right_on='customer_id', how='left', suffixes=('', '_cust'))

    # Transform columns
    result = merged_df[[
        'sls_ord_num',
        'product_key',
        'customer_key',
        'sls_order_dt',
        'sls_ship_dt',
        'sls_due_dt',
        'sls_sales',
        'sls_quantity',
        'sls_price'
    ]].rename(columns={
        'sls_ord_num': 'order_number',
        'sls_order_dt': 'order_date',
        'sls_ship_dt': 'ship_date',
        'sls_due_dt': 'due_date',
        'sls_sales': 'sales_amount',
        'sls_quantity': 'quantity',
        'sls_price': 'price'
    })

    # Enforce data types
    result = result.astype({
        'order_number': 'object',
        'product_key': 'object',
        'customer_key': 'object',
        'sales_amount': 'float64',
        'quantity': 'int64',
        'price': 'float64'
    })

    # Convert datetime columns
    for col in ['order_date', 'ship_date', 'due_date']:
        result[col] = pd.to_datetime(result[col], errors='coerce')

    # Add dwh_create_date with current timestamp
    result['dwh_create_date'] = pd.Timestamp.now()

    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "gold_fact_sales",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )