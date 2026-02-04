{{ config(schema='gold' , materialized='table',
post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (card_id)"
    ]

) }}

WITH silver_cards AS (
    SELECT * FROM {{ ref('cards') }}
)

SELECT
    card_id,
    user_id,
    card_brand,
    standardized_card_type,
    -- Security indicator: 1 if card was found on dark web, else 0
    is_on_dark_web,
    -- Date info to see if older cards are more vulnerable
    card_expires AS expiration_date,
    num_cards_issued AS num_cards_issued,
    credit_limit_usd AS credit_limit_usd,
    opened_at AS opened_at,
    pin_last_changed_year AS pin_last_changed_year
FROM silver_cards