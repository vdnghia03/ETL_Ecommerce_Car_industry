{{ config(materialized='view') }}

SELECT
    CAST(order_number AS VARCHAR(50)) AS order_number
    , CAST(product_key AS BIGINT) AS product_key
    , CAST(customer_key AS BIGINT) AS customer_key
    , CAST(order_date AS DATE) AS order_date
    , CAST(ship_date AS DATE) AS ship_date
    , CAST(due_date AS DATE) AS due_date
    , CAST(sales_amount AS DOUBLE PRECISION) AS sales_amount
    , CAST(quantity AS INT) AS quantity
    , CAST(price AS DOUBLE PRECISION) AS price
FROM {{ source('warehouse', 'warehouse_fact_sales') }}
