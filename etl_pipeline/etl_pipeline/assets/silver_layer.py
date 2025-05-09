import pandas as pd
from dagster import asset, Output, AssetIn
import logging



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