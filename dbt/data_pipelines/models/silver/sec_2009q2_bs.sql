
{{ config(
    materialized='table',
    unique_key='bs_id',
    pre_hook="CREATE SEQUENCE IF NOT EXISTS bs_seq START WITH 1 INCREMENT BY 1"
) }}

WITH source AS (
    SELECT
        id AS main_id,
        bs.value AS bs_json
    FROM {{ source('airflow_s3', 'sec_2009q2') }},
    LATERAL FLATTEN(input => data:"bs") AS bs
),

sec_2009q2_bs AS (
    SELECT 
        bs_seq.NEXTVAL AS bs_id,
        main_id,
        bs_json:"concept" AS concept,
        bs_json:"info" AS info,
        bs_json:"label" AS label,
        bs_json:"unit" AS unit,
        bs_json:"value"::float AS value
    FROM source
)

SELECT * FROM sec_2009q2_bs

