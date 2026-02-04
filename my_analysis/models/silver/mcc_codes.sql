-- models/silver/silver_mcc_codes.sql
{{ config(schema='silver', materialized='table') }}

SELECT
    id::INT AS mcc_code,   -- Casting to INT to match transactions table
    value AS mcc_description
FROM {{ source('bronze_data', 'mcc_codes') }}
WHERE id IS NOT NULL