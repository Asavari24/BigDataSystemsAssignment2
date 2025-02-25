select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test for unique values in main_id in sec_2016q4_main
SELECT MAIN_ID FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_main
GROUP BY MAIN_ID
HAVING COUNT(*) > 1
      
    ) dbt_internal_test