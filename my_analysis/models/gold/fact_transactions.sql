{{ config(
    schema='gold',
    materialized='table',
    post_hook=[
      
      "ALTER TABLE {{ this }} ADD PRIMARY KEY (trans_id)",
      
      
      "CREATE INDEX IF NOT EXISTS idx_fact_trans_at ON {{ this }} (trans_at)",
      "CREATE INDEX IF NOT EXISTS idx_fact_user_id ON {{ this }} (user_id)",
      "CREATE INDEX IF NOT EXISTS idx_fact_mcc_code ON {{ this }} (mcc_code)",
      "CREATE INDEX IF NOT EXISTS idx_fact_is_fraud ON {{ this }} (is_fraud)",  

      
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES {{ ref('dim_users') }} (user_id) NOT VALID",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_card FOREIGN KEY (card_id) REFERENCES {{ ref('dim_cards') }} (card_id) NOT VALID",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_mcc FOREIGN KEY (mcc_code) REFERENCES {{ ref('dim_mcc_codes') }} (mcc_code) NOT VALID",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_date FOREIGN KEY (trans_date) REFERENCES {{ ref('dim_date') }} (date_day) NOT VALID"
    ]
) }}

WITH t AS (
    SELECT * FROM {{ ref('transactions') }} 
),
f AS (
    SELECT * FROM {{ ref('fraud_labels') }} 
)

SELECT
    t.trans_id,
    t.user_id,
    t.card_id,
    t.mcc_code,
    t.trans_at::date AS trans_date, 
    t.trans_at,
    t.amount_usd,
    t.trans_type,
    t.country,
    t.city,
    COALESCE(f.is_fraud, 0) AS is_fraud
FROM t
LEFT JOIN f ON t.trans_id = f.transaction_id