-- Test for non-null values in main_id in sec_2016q4_main
SELECT main_id 
FROM {{ref ('sec_2016q4_main')}}
WHERE main_id IS NULL
