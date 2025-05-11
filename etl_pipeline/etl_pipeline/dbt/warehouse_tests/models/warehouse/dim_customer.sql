{{ config(materialized='view') }}

SELECT * FROM {{ source('warehouse', 'warehouse_dim_customer') }}

