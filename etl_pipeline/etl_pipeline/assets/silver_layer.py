import pandas as pd
from dagster import asset, Output, AssetIn
import logging


# ===========================================================
# crm_cust_info
# ===========================================================

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "crm_cust_info": AssetIn(
            key_prefix=["ERM", "bronze"]
        ),
    },
    key_prefix=["ERM", "silver"],
    description='Insert transform and Cleaned data crm_cust_info from Bronze into Silver table',
    group_name="Silver_layer",
    compute_kind="Pandas"
)
def silver_crm_cust_info(crm_cust_info: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create a copy of the input DataFrame to avoid modifying the original
    cust_info = crm_cust_info.copy()

    # Step 1: Convert cst_create_date from string to datetime, invalid values become NaT
    cust_info['cst_create_date'] = pd.to_datetime(cust_info['cst_create_date'], errors='coerce')

    # Step 2: Filter out records where cst_id is null or cst_id = 0 or empty
    cust_info = cust_info[cust_info['cst_id'].notnull()]
    cust_info = cust_info[cust_info['cst_id'] != 0]
    cust_info = cust_info[cust_info['cst_id'].astype(str).str.strip() != '']

    # Step 3: Deduplicate by keeping the most recent record for each cst_id
    # Sort by cst_create_date descending and group by cst_id to get the latest record
    cust_info = (cust_info
                 .sort_values(by=['cst_create_date'], ascending=False)
                 .groupby('cst_id')
                 .first()
                 .reset_index())

    # Step 4: Transform the data
    # Remove unnecessary spaces from cst_firstname and cst_lastname
    cust_info['cst_firstname'] = cust_info['cst_firstname'].str.strip()
    cust_info['cst_lastname'] = cust_info['cst_lastname'].str.strip()

    # Normalize marital status values
    cust_info['cst_marital_status'] = cust_info['cst_marital_status'].str.strip().str.upper().map({
        'S': 'Single',
        'M': 'Married'
    }).fillna('n/a')

    # Normalize gender values
    cust_info['cst_gndr'] = cust_info['cst_gndr'].str.strip().str.upper().map({
        'F': 'Female',
        'M': 'Male'
    }).fillna('n/a')

    # Step 5: Select the required columns
    result = cust_info[[
        'cst_id',
        'cst_key',
        'cst_firstname',
        'cst_lastname',
        'cst_marital_status',
        'cst_gndr',
        'cst_create_date'
    ]]

    # Step 6: Enforce data types to match SQL table schema
    result = result.astype({
        'cst_id': 'int64',              # INT
        'cst_key': 'object',            # NVARCHAR(50)
        'cst_firstname': 'object',      # NVARCHAR(50)
        'cst_lastname': 'object',       # NVARCHAR(50)
        'cst_marital_status': 'object', # NVARCHAR(50)
        'cst_gndr': 'object',           # NVARCHAR(50)
    })



    # Add dwh_create_date with current timestamp (equivalent to GETDATE())
    result['dwh_create_date'] = pd.Timestamp.now()


    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "silver_crm_cust_info",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )

# ===========================================================
# crm_prd_info
# ===========================================================

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "crm_prd_info": AssetIn(
            key_prefix=["ERM", "bronze"]
        ),
    },
    key_prefix=["ERM", "silver"],
    description='Insert transform and Cleaned data crm_prd_info from Bronze into Silver table',
    group_name="Silver_layer",
    compute_kind="Pandas"
)
def silver_crm_prd_info(crm_prd_info: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create a copy of the input DataFrame to avoid modifying the original
    prd_info = crm_prd_info.copy()

    # Step 1: Convert prd_start_dt from string to datetime, invalid values become NaT
    prd_info['prd_start_dt'] = pd.to_datetime(prd_info['prd_start_dt'], errors='coerce')

    # Step 2: Transform the data
    # cat_id: Take first 5 characters of prd_key, replace '-' with '_'
    prd_info['cat_id'] = prd_info['prd_key'].str.strip().str[:5].str.replace('-', '_')

    # prd_key: Take from character 7 onwards
    prd_info['prd_key'] = prd_info['prd_key'].str.strip().str[6:]

    # prd_nm: Keep as is (already in correct format)
    prd_info['prd_nm'] = prd_info['prd_nm']

    # prd_cost: Replace NaN with 0 and convert to int
    prd_info['prd_cost'] = prd_info['prd_cost'].fillna(0).astype('int64')

    # prd_line: Normalize values
    prd_info['prd_line'] = prd_info['prd_line'].str.strip().str.upper().map({
        'M': 'Mountain',
        'R': 'Road',
        'S': 'Other Sales',
        'T': 'Touring'
    }).fillna('n/a')

    # prd_end_dt: Use LEAD equivalent in Pandas
    # Sort by prd_key and prd_start_dt, then shift prd_start_dt to get the next start date, subtract 1 day
    prd_info = prd_info.sort_values(by=['prd_key', 'prd_start_dt'])
    prd_info['prd_end_dt'] = (prd_info.groupby('prd_key')['prd_start_dt']
                              .shift(-1)
                              .apply(lambda x: x - pd.Timedelta(days=1) if pd.notnull(x) else pd.NaT))

    # Step 3: Select the required columns
    result = prd_info[[
        'prd_id',
        'cat_id',
        'prd_key',
        'prd_nm',
        'prd_cost',
        'prd_line',
        'prd_start_dt',
        'prd_end_dt'
    ]]

    # Step 4: Enforce basic data types (only for non-datetime columns)
    result = result.astype({
        'prd_id': 'int64',         # INT
        'cat_id': 'object',        # NVARCHAR(50)
        'prd_key': 'object',       # NVARCHAR(50)
        'prd_nm': 'object',        # NVARCHAR(50)
        'prd_cost': 'int64',       # INT
        'prd_line': 'object',      # NVARCHAR(50)
    })

    # Add dwh_create_date with current timestamp (equivalent to GETDATE())
    result['dwh_create_date'] = pd.Timestamp.now()

    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "silver_crm_prd_info",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )


# ==========================================================
# crm_sales_details
# ==========================================================

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "crm_sales_details": AssetIn(
            key_prefix=["ERM", "bronze"]
        ),
    },
    key_prefix=["ERM", "silver"],
    description='Insert transform and Cleaned data crm_sales_details from Bronze into Silver table',
    group_name="Silver_layer",
    compute_kind="Pandas"
)
def silver_crm_sales_details(crm_sales_details: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create a copy of the input DataFrame to avoid modifying the original
    sales_info = crm_sales_details.copy()

    # Step 1: Transform datetime columns (sls_order_dt, sls_ship_dt, sls_due_dt)
    # Convert to string to check length, then to datetime if valid
    for col in ['sls_order_dt', 'sls_ship_dt', 'sls_due_dt']:
        # Convert to string, handle numeric input (YYYYMMDD format)
        sales_info[col] = sales_info[col].astype(str)
        # Set to NaT if value is '0' or length != 8
        sales_info[col] = sales_info[col].apply(lambda x: x if x != '0' and len(x) == 8 else pd.NaT)
        # Convert valid strings (YYYYMMDD) to datetime
        sales_info[col] = pd.to_datetime(sales_info[col], format='%Y%m%d', errors='coerce')

    # Step 2: Recalculate sls_sales
    # If sls_sales <= 0, is null, or not equal to ABS(sls_price) * sls_quantity, then recalculate
    sales_info['sls_sales'] = sales_info.apply(
        lambda row: abs(row['sls_price']) * row['sls_quantity']
        if (row['sls_sales'] <= 0 or pd.isna(row['sls_sales']) or row['sls_sales'] != abs(row['sls_price']) * row['sls_quantity'])
        else row['sls_sales'],
        axis=1
    )

    # Step 3: Derive sls_price
    # If sls_price <= 0 or is null, calculate as sls_sales / sls_quantity (avoid division by 0)
    sales_info['sls_price'] = sales_info.apply(
        lambda row: row['sls_sales'] / row['sls_quantity'] if (row['sls_price'] <= 0 or pd.isna(row['sls_price'])) and row['sls_quantity'] != 0
        else row['sls_price'],
        axis=1
    )

    # Step 4: Select the required columns
    result = sales_info[[
        'sls_ord_num',
        'sls_prd_key',
        'sls_cust_id',
        'sls_order_dt',
        'sls_ship_dt',
        'sls_due_dt',
        'sls_sales',
        'sls_quantity',
        'sls_price'
    ]]

    # Step 5: Enforce data types to match SQL table schema
    result = result.astype({
        'sls_ord_num': 'object',   # NVARCHAR(50)
        'sls_prd_key': 'object',   # NVARCHAR(50)
        'sls_cust_id': 'int64',    # INT
        'sls_sales': 'int64',      # INT
        'sls_quantity': 'int64',   # INT
        'sls_price': 'int64'       # INT
    })

    # Add dwh_create_date with current timestamp (equivalent to GETDATE())
    result['dwh_create_date'] = pd.Timestamp.now()

    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "silver_crm_sales_details",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )

# ============================================================
# erp_cust_az12
# ============================================================

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "erp_cust_az12": AssetIn(
            key_prefix=["ERM", "bronze"]
        ),
    },
    key_prefix=["ERM", "silver"],
    description='Insert transform and Cleaned data erp_cust_az12 from Bronze into Silver table',
    group_name="Silver_layer",
    compute_kind="Pandas"
)
def silver_erp_cust_az12(erp_cust_az12: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create a copy of the input DataFrame to avoid modifying the original
    cust_info = erp_cust_az12.copy()

    # Step 1: Transform cid
    # Remove 'NAS' prefix if it exists
    cust_info['cid'] = cust_info['cid'].apply(lambda x: x[3:] if str(x).startswith('NAS') else x)

    # Step 2: Transform bdate
    # Convert bdate to datetime, invalid values become NaT
    cust_info['bdate'] = pd.to_datetime(cust_info['bdate'], errors='coerce')
    # Set bdate to NaT if it's in the future (compared to current date)
    current_date = pd.Timestamp.now()
    cust_info['bdate'] = cust_info['bdate'].apply(lambda x: x if pd.isna(x) or x <= current_date else pd.NaT)

    # Step 3: Normalize gen
    # Standardize gender values
    cust_info['gen'] = (cust_info['gen']
                        .str.strip()
                        .str.upper()
                        .replace({'M': 'Male', 'MALE': 'Male', 'F': 'Female', 'FEMALE': 'Female'})
                        .fillna('n/a')
                        .where(cust_info['gen'].str.strip().str.upper().isin(['M', 'MALE', 'F', 'FEMALE']), 'n/a'))

    # Step 4: Select the required columns
    result = cust_info[[
        'cid',
        'bdate',
        'gen'
    ]]

    # Step 5: Enforce data types to match SQL table schema
    result = result.astype({
        'cid': 'object',  # NVARCHAR(50)
        'gen': 'object'   # NVARCHAR(50)
    })

    # # Convert bdate to date only (match DATE type)
    # result['bdate'] = result['bdate'].dt.date

    # Add dwh_create_date with current timestamp (equivalent to GETDATE())
    result['dwh_create_date'] = pd.Timestamp.now()

    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "silver_erp_cust_az12",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )

# ============================================================
# erp_loc_a101
# ============================================================

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "erp_loc_a101": AssetIn(
            key_prefix=["ERM", "bronze"]
        ),
    },
    key_prefix=["ERM", "silver"],
    description='Insert transform and Cleaned data erp_loc_a101 from Bronze into Silver table',
    group_name="Silver_layer",
    compute_kind="Pandas"
)
def silver_erp_loc_a101(erp_loc_a101: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create a copy of the input DataFrame to avoid modifying the original
    loc_info = erp_loc_a101.copy()

    # Step 1: Transform cid
    # Remove whitespace and replace '-' with empty string
    loc_info['cid'] = loc_info['cid'].str.strip().str.replace('-', '')

    # Step 2: Normalize cntry
    # Standardize country values
    loc_info['cntry'] = (loc_info['cntry']
                         .str.strip()
                         .replace({'DE': 'Germany', 'US': 'United States', 'USA': 'United States'})
                         .fillna('n/a')
                         .where(loc_info['cntry'].str.strip().isin(['DE', 'US', 'USA', '']), loc_info['cntry'].str.strip())
                         .where(~loc_info['cntry'].str.strip().isin(['', None]), 'n/a'))

    # Step 3: Apply DISTINCT (remove duplicates based on all columns)
    result = loc_info.drop_duplicates()

    # Step 4: Select the required columns
    result = result[[
        'cid',
        'cntry'
    ]]

    # Step 5: Enforce basic data types
    result = result.astype({
        'cid': 'object',   # NVARCHAR
        'cntry': 'object'  # NVARCHAR
    })

    # Add dwh_create_date with current timestamp (equivalent to GETDATE())
    result['dwh_create_date'] = pd.Timestamp.now()

    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "silver_erp_loc_a101",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )

# ============================================================
# erp_px_cat_g1v2
# ============================================================


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "erp_px_cat_g1v2": AssetIn(
            key_prefix=["ERM", "bronze"]
        ),
    },
    key_prefix=["ERM", "silver"],
    description='Insert transform and Cleaned data erp_px_cat_g1v2 from Bronze into Silver table',
    group_name="Silver_layer",
    compute_kind="Pandas"
)
def silver_erp_px_cat_g1v2(erp_px_cat_g1v2: pd.DataFrame) -> Output[pd.DataFrame]:
    # Create a copy of the input DataFrame to avoid modifying the original
    cat_info = erp_px_cat_g1v2.copy()


    # Step 1: Transform
    # cat: Remove leading and trailing spaces
    cat_info['cat'] = cat_info['cat'].str.strip()
    # subcat: Remove leading and trailing spaces
    cat_info['subcat'] = cat_info['subcat'].str.strip()
    # maintenance: Remove leading and trailing spaces
    cat_info['maintenance'] = cat_info['maintenance'].str.strip()

    # Step 2: Select the required columns
    result = cat_info[[
        'id',
        'cat',
        'subcat',
        'maintenance'
    ]]

    # Step 3: Enforce data types to match SQL table schema
    result = result.astype({
        'id': 'object',         # NVARCHAR(50)
        'cat': 'object',        # NVARCHAR(50)
        'subcat': 'object',     # NVARCHAR(50)
        'maintenance': 'object' # NVARCHAR(50)
    })

    # Add dwh_create_date with current timestamp (equivalent to GETDATE())
    result['dwh_create_date'] = pd.Timestamp.now()

    # Prepare metadata including column data types
    column_dtypes = {col: str(result[col].dtype) for col in result.columns}
    metadata = {
        "table": "silver_erp_px_cat_g1v2",
        "records": len(result),
        "column_dtypes": column_dtypes
    }

    # Return the transformed DataFrame with metadata
    return Output(
        result,
        metadata=metadata
    )