-- models/silver/silver_fraud_labels.sql
{{ config(schema='silver', materialized='table') }}
SELECT
    id::BIGINT AS transaction_id,
    -- FIX: Convert "Yes"/"No" text to 1/0 integers
    CASE 
        WHEN LOWER(value) IN ('yes', 'y', '1', 'true') THEN 1
        WHEN LOWER(value) IN ('no', 'n', '0', 'false') THEN 0
        ELSE value::INT  -- If already numeric, cast it
    END AS is_fraud
FROM {{ source('bronze_data', 'fraud_labels') }}
WHERE id IS NOT NULL