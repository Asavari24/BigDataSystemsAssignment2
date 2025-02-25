select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test for non-null values in main_id in sec_2016q4_main
SELECT main_id 
FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_main
WHERE main_id IS NULL
      
    ) dbt_internal_test