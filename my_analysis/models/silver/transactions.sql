{{ config(schema='silver', materialized='table') }}

WITH source_data AS (
    -- Import raw transaction data from the bronze layer
    SELECT * FROM {{ source('bronze_data', 'transactions') }}
),

transformed_amounts AS (
    -- Intermediate step to clean currency and cast to numeric for logical operations
    SELECT 
        *,
        CASE 
            WHEN amount IS NULL THEN 0
            -- Remove currency symbols ($) and commas, then cast to decimal
            ELSE CAST(REPLACE(REPLACE(amount, '$', ''), ',', '') AS DECIMAL(18, 2))
        END AS amount_usd
    FROM source_data
),

cleaned_data AS (
    SELECT
        -- Primary identity of the transaction
        id::BIGINT AS trans_id,
        client_id::BIGINT AS user_id,
        card_id::BIGINT AS card_id,
        merchant_id::BIGINT AS merchant_id,
        
        -- Timestamp of the transaction
        date AS trans_at,
        
        -- Cleaned transaction amount
        amount_usd,

        -- Transaction Classification: Distinguish between refunds (negative) and purchases
        CASE 
            WHEN amount_usd < 0 THEN 'Refund'
            ELSE 'Purchase'
        END AS trans_category,
        
        -- Standardizing transaction type by removing redundant suffix
        REPLACE(use_chip, ' Transaction', '') AS trans_type,

        -- GEOGRAPHY LOGIC:
        -- 1. Country Classification: Handle Online/NULL as 'E-Commerce' and identify 'USA'
        CASE 
            WHEN merchant_state IS NULL OR UPPER(TRIM(merchant_state)) = 'ONLINE' THEN 'E-Commerce'
            WHEN LENGTH(merchant_state) = 2 THEN 'USA'
            ELSE merchant_state 
        END AS country,

        -- 2. City Classification: Standardize Online/NULL cities to 'E-Commerce'
        CASE 
            WHEN UPPER(TRIM(merchant_city)) = 'ONLINE' OR merchant_state IS NULL THEN 'E-Commerce'
            ELSE merchant_city 
        END AS city,

        -- 3. State Extraction: Isolate 2-letter codes for US states only
        CASE 
            WHEN LENGTH(merchant_state) = 2 THEN merchant_state
            ELSE NULL 
        END AS us_state_code,

        -- 4. ZIP Code Standardization: Ensure 5-digit format for USA and handle missing values
        CASE 
            WHEN zip IS NULL THEN 'Missing'
            WHEN LENGTH(merchant_state) = 2 THEN LPAD(SPLIT_PART(zip::TEXT, '.', 1), 5, '0')
            ELSE SPLIT_PART(zip::TEXT, '.', 1)
        END AS zip_code,
        
        -- Merchant Category Code (MCC)
        mcc::BIGINT AS mcc_code,
        
        -- Error handling: Replace NULL errors with 'Success' status
        COALESCE(errors, 'Success') AS status_message

    FROM transformed_amounts
)

SELECT * FROM cleaned_data
-- Final Integrity Filter: Exclude rows missing critical identifiers
WHERE trans_id IS NOT NULL 
  AND user_id IS NOT NULL