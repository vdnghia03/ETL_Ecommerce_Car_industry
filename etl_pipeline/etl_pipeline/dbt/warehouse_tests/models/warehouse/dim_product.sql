{{ config(materialized='view') }}

SELECT
    CAST(product_key AS BIGINT) AS product_key
    , CAST(product_id AS INT) AS product_id
    , CAST(product_number AS VARCHAR(50)) AS product_number
    , CAST(product_name AS VARCHAR(50)) AS product_name
    , CAST(category_id AS VARCHAR(50)) AS category_id
    , CAST(category AS VARCHAR(50)) AS category
    , CAST(subcategory AS VARCHAR(50)) AS subcategory
    , CAST(maintenance AS VARCHAR(50)) AS maintenance
    , CAST(cost AS DOUBLE PRECISION) AS cost
    , CAST(product_line AS VARCHAR(50)) AS product_line
    , CAST(start_date AS DATE) AS start_date
FROM {{ source('warehouse', 'warehouse_dim_product') }}
