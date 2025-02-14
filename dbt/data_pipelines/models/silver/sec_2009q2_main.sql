

{{ config(
    materialized='table',
    unique_key='main_id'
) }}

WITH sec_2009q2_main AS (
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
    FROM {{ source('airflow_s3', 'sec_2009q2') }}
)

SELECT * FROM sec_2009q2_main

