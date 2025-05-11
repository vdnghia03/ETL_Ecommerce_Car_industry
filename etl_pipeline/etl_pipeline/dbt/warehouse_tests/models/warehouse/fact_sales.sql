{{ config(materialized='view') }}

SELECT * FROM {{ source('warehouse', 'warehouse_fact_sales') }}
