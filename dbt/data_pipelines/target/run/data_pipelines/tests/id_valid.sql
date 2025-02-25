select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test for uniqueness and non-null values in sec_2016q4_main
SELECT MAIN_ID FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_main
GROUP BY MAIN_ID
HAVING COUNT(*) > 1;
 
SELECT COUNT(*) FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_main
WHERE MAIN_ID IS NULL;



-- Test for uniqueness and non-null values in sec_2016q4_bs
SELECT BS_ID FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_bs
GROUP BY BS_ID
HAVING COUNT(*) > 1;
 
SELECT COUNT(*) FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_bs
WHERE BS_ID IS NULL;



-- Test for uniqueness and non-null values in sec_2016q4_cf
SELECT CF_ID FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_cf
GROUP BY CF_ID
HAVING COUNT(*) > 1;
 
SELECT COUNT(*) FROM BIGDATASYSTEMS_DB.AIRFLOW_DBT.sec_2016q4_cf
WHERE CF_ID IS NULL;
      
    ) dbt_internal_test