-- ===============================================
-- Build Bronze Layer for Data Warehouse (PostgreSQL Version)
-- ===============================================
-- Script Purpose:
--   - Create schema "bronze"
--   - Drop tables if exist
--   - Create tables with appropriate columns

-- Switch to ERM database: 
-- (In PostgreSQL, you usually do this from the client/connection, not inside SQL)
-- \c ERM -- This is done outside SQL scripts in most tools

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS bronze;

-- Set the search path to the bronze schema
SET search_path TO bronze;

-- Data Ingestion

DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
    cst_id INTEGER,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
    prd_id INTEGER,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INTEGER,
    prd_line VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt TIMESTAMP
);

DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INTEGER,
    sls_order_dt INTEGER,
    sls_ship_dt INTEGER,
    sls_due_dt INTEGER,
    sls_sales INTEGER,
    sls_quantity INTEGER,
    sls_price INTEGER
);

DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);

DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);
