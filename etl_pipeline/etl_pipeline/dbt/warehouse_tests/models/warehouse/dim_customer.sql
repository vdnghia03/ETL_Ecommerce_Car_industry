{{ config(materialized='view') }}

SELECT 
    CAST(customer_key AS BIGINT) AS customer_key
    , CAST(customer_id AS VARCHAR(50)) AS customer_id
    , CAST(customer_number AS VARCHAR(50)) AS customer_number
    , CAST(first_name AS VARCHAR(50)) AS first_name
    , CAST(last_name AS VARCHAR(50)) AS last_name
    , CAST(country AS VARCHAR(50)) AS country
    , CAST(marital_status AS VARCHAR(50)) AS marital_status
    , CAST(gender AS VARCHAR(50)) AS gender
    , CAST(birthday AS DATE) AS birthday
    , CAST(create_date AS DATE) AS create_date
FROM {{ source('warehouse', 'warehouse_dim_customer') }}

