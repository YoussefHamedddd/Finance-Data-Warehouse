{{ config(schema='gold', materialized='table' ,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (date_day)"
    ]
    ) }}


WITH date_range AS (
    SELECT 
        MIN(trans_at)::date AS start_date,
        (MAX(trans_at)::date + interval '1 day')::date AS end_date
    FROM {{ ref('transactions') }}
),


generated_dates AS (
    SELECT 
        generate_series(start_date, end_date, '1 day'::interval)::date AS date_day
    FROM date_range
)


SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'TMDay') AS day_name, -- 'TMDay' 
    EXTRACT(DAY FROM date_day) AS day_of_month,
    CASE WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generated_dates