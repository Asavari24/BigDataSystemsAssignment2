
  
    

        create or replace transient table BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_ic
         as
        (

WITH source AS (
    SELECT
        id AS main_id,
        ic.value AS ic_json
    FROM bigdatasystems_db.airflow_s3.sec_2016q4,
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
        );
      
  