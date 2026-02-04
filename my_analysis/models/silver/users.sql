-- models/silver/silver_users.sql
{{ config(schema='silver', materialized='table') }}

WITH source_users AS (
    -- Access raw users data from Bronze
    SELECT * FROM {{ source('bronze_data', 'users') }}
),

cleaned_users AS (
    SELECT
        id::BIGINT AS user_id,
        current_age::INT AS current_age,
        retirement_age::INT AS retirement_age,
        birth_year::INT AS birth_year,
        birth_month::INT AS birth_month,
        gender,
        
        -- Cleaning Currency: Removing '$' from income and debt columns and rounding
        CAST(ROUND(CAST(REPLACE(REPLACE(per_capita_income, '$', ''), ',', '') AS DECIMAL(18, 2))) AS BIGINT) AS per_capita_income_usd,
        CAST(ROUND(CAST(REPLACE(REPLACE(yearly_income, '$', ''), ',', '') AS DECIMAL(18, 2))) AS BIGINT) AS yearly_income_usd,
        CAST(ROUND(CAST(REPLACE(REPLACE(total_debt, '$', ''), ',', '') AS DECIMAL(18, 2))) AS BIGINT) AS total_debt_usd,
        
        credit_score::INT AS credit_score,
        num_credit_cards::INT AS num_cards,
        
        -- Geography & Contact
        address,
        latitude::DOUBLE PRECISION AS latitude,
        longitude::DOUBLE PRECISION AS longitude
    FROM source_users
)

SELECT * FROM cleaned_users
WHERE user_id IS NOT NULL