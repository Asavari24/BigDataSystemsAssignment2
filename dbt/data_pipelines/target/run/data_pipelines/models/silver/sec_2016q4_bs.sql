
  
    

        create or replace transient table BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_bs
         as
        (

WITH source AS (
    SELECT
        id AS main_id,
        bs.value AS bs_json
    FROM bigdatasystems_db.airflow_s3.sec_2016q4,
    LATERAL FLATTEN(input => data:"bs") AS bs
),

sec_2016q4_bs AS (
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

SELECT * FROM sec_2016q4_bs
        );
      
  