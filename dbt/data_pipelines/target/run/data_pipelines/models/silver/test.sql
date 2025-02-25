
  create or replace   view BIGDATASYSTEMS_DB.AIRFLOW_DBT.test
  
   as (
    select * from bigdatasystems_db.airflow_s3.sec_2009q2
  );

