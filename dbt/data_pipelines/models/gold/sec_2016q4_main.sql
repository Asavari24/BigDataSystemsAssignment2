

{{ config(
    materialized='table',
    unique_key='main_id'
) }}

WITH sec_2016q4_main AS (
    SELECT 
        id AS main_id,
        city,
        country,
        enddate,
        name,
        secquarter,
        startdate,
        symbol,
        secyear
    FROM {{ source('airflow_s3', 'sec_2016q4') }}
)

SELECT * FROM sec_2016q4_main

