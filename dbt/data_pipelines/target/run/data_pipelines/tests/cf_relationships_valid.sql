select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test foreign key relationship between sec_2016q4_cf and sec_2016q4_main
SELECT MAIN_ID FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_cf
WHERE MAIN_ID NOT IN (SELECT MAIN_ID FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_main)
      
    ) dbt_internal_test