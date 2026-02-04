-- models/silver/silver_cards.sql
{{ config(schema='silver', materialized='table') }}
WITH source_cards AS (
    SELECT * FROM {{ source('bronze_data', 'cards') }}
),
cleaned_cards AS (
    SELECT
        id::BIGINT AS card_id,
        client_id::BIGINT AS user_id,
        card_brand,
        CASE 
        WHEN card_type ILIKE '%prepaid%' THEN 'Prepaid Debit'
        WHEN card_type ILIKE 'debit' THEN 'Standard Debit'
        ELSE card_type 
        END AS standardized_card_type,
        -- Convert to DECIMAL first to handle the scientific notation, then to BIGINT for display
        CAST(card_number AS DECIMAL(20,0))::BIGINT AS card_number ,
        
        -- FIX: Clean credit_limit by removing '$' and ',' before conversion and rounding to BIGINT 
        CAST(ROUND(CAST(REPLACE(REPLACE(credit_limit, '$', ''), ',', '') AS DECIMAL(18, 2))) AS BIGINT) AS credit_limit_usd,
        
        cvv::BIGINT AS cvv,
        has_chip AS has_chip,
        num_cards_issued::BIGINT AS num_cards_issued,
        
        -- FIX: Handle date format like "Sep-02" by converting to proper date
        -- Using TO_DATE with format 'Mon-YY' 
        -- Adding a CASE to handle potential variations
        CASE 
            -- first case:09/2002
            WHEN acct_open_date ~ '^[0-9]{2}/[0-9]{4}$' THEN 
                TO_DATE(acct_open_date, 'MM/YYYY')
            
            -- second case: Sep-02
            WHEN acct_open_date ~ '^[A-Za-z]{3}-[0-9]{2}$' THEN 
                TO_DATE(acct_open_date, 'Mon-YY')
            
            -- Default case: try casting directly
            ELSE 
                acct_open_date::DATE
        END AS opened_at,
        
        expires::VARCHAR AS card_expires,
        year_pin_last_changed::BIGINT AS pin_last_changed_year,
        
        card_on_dark_web AS is_on_dark_web
    FROM source_cards
)
SELECT * FROM cleaned_cards
WHERE card_id IS NOT NULL