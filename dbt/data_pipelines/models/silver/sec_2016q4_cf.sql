
{{ config(
    materialized='table',
    unique_key='cf_id',
    pre_hook="CREATE SEQUENCE IF NOT EXISTS cf_seq START WITH 1 INCREMENT BY 1"
) }}

WITH source AS (
    SELECT
        id AS main_id,
        cf.value AS cf_json
    FROM {{ source('airflow_s3', 'sec_2016q4') }},
    LATERAL FLATTEN(input => data:"cf") AS cf
),

sec_2016q4_cf AS (
    SELECT 
        cf_seq.NEXTVAL AS cf_id,
        main_id,
        cf_json:"concept" AS concept,
        cf_json:"info" AS info,
        cf_json:"label" AS label,
        cf_json:"unit" AS unit,
        cf_json:"value"::float AS value
    FROM source
)

SELECT * FROM sec_2016q4_cf