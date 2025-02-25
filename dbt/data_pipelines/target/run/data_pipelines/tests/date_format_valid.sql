select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test for valid date formats in sec_2016q4_main
SELECT ENDDATE FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_main
WHERE TRY_TO_DATE(ENDDATE, 'YYYY-MM-DD') IS NULL
 
--SELECT STARTDATE FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_main
--WHERE TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD') IS NULL;
      
    ) dbt_internal_test