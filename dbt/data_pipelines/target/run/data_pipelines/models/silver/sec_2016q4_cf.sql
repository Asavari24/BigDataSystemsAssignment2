
  
    

        create or replace transient table BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_cf
         as
        (

WITH source AS (
    SELECT
        id AS main_id,
        cf.value AS cf_json
    FROM bigdatasystems_db.airflow_s3.sec_2016q4,
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
        );
      
  