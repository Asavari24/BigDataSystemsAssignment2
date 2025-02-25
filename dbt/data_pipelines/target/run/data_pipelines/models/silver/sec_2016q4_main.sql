
  
    

        create or replace transient table BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_main
         as
        (

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
    FROM bigdatasystems_db.airflow_s3.sec_2016q4
)

SELECT * FROM sec_2016q4_main
        );
      
  