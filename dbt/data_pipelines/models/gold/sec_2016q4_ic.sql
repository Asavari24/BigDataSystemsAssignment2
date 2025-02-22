
{{ config(
    materialized='table',
    unique_key='ic_id',
    pre_hook="CREATE SEQUENCE IF NOT EXISTS ic_seq START WITH 1 INCREMENT BY 1"
) }}

WITH source AS (
    SELECT
        id AS main_id,
        ic.value AS ic_json
    FROM {{ source('airflow_s3', 'sec_2016q4') }},
    LATERAL FLATTEN(input => data:"ic") AS ic
),

sec_2016q4_ic AS (
    SELECT 
        ic_seq.NEXTVAL AS ic_id,
        main_id,
        ic_json:"concept" AS concept,
        ic_json:"info" AS info,
        ic_json:"label" AS label,
        ic_json:"unit" AS unit,
        ic_json:"value"::float AS value
    FROM source
)

SELECT * FROM sec_2016q4_ic