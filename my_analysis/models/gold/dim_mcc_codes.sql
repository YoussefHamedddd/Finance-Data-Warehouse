{{ config(
    schema='gold', 
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (mcc_code)"
    ]
) }}

WITH unique_mccs AS (
   
    SELECT DISTINCT mcc_code 
    FROM {{ ref('transactions') }}
)

SELECT 
    mcc_code,
    CASE 
        WHEN mcc_code BETWEEN 7000 AND 7299 THEN 'Lodging & Hotels'
        WHEN mcc_code BETWEEN 5800 AND 5899 THEN 'Eating & Drinking (Restaurants)'
        WHEN mcc_code BETWEEN 5400 AND 5499 THEN 'Food Stores (Supermarkets)'
        WHEN mcc_code BETWEEN 4000 AND 4799 THEN 'Transportation'
        WHEN mcc_code BETWEEN 6000 AND 6999 THEN 'Financial Services'
        WHEN mcc_code BETWEEN 4800 AND 4999 THEN 'Utilities'
        WHEN mcc_code BETWEEN 5000 AND 5599 THEN 'Retail Outlets'
        ELSE 'Other Services'
    END AS mcc_category,
    
    'Category for ' || mcc_code::text AS mcc_description
FROM unique_mccs