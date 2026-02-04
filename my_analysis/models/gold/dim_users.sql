{{ config(schema='gold', materialized='table' ,
post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (user_id)"
    ]
 )}}

WITH silver_users AS (
    --  Bringing cleaned users from Silver
    SELECT * FROM {{ ref('users') }}
)

SELECT
    user_id,
    current_age,
    gender,
    -- Financial Info
    yearly_income_usd,
    per_capita_income_usd,
    total_debt_usd,
    credit_score,
    -- data about family planning
    num_cards,
    birth_year,
    retirement_age,
    --- Geography & Contact Info
    address AS user_address,
    latitude,
    longitude
FROM silver_users