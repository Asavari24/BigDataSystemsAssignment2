from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from web_scrapper_s3 import finance_data_scrapper
from text_to_json_converter import get_converted_files
from airflow.operators.bash import BashOperator


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1)
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'web_snowflake_dbt_dag',
    default_args=default_args,
    description='Scrape web data, load to S3 and Snowflake using dbt',
    schedule_interval=None,
    catchup=False
)

dbt_path = "/opt/dbt"





def aws_conn():
    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='aws_default')

    try:
        # List objects in bucket to verify connection
        bucket_name = 'bigdatasystems2'
        object_list = s3_hook.list_keys(bucket_name=bucket_name)
        print("Connection successful!")
        print(f"Objects found: {object_list[:5]}")
    except Exception as e:
        print(f"Connection failed: {str(e)}")



# Test AWS connection
test_conn_aws = PythonOperator(
    task_id='test_aws_connection',
    python_callable=aws_conn,
    #provide_context=True,
    dag=dag
)


# Test Snowflake connection
test_conn_snowflake = SQLExecuteQueryOperator(
    task_id='test_snowflake_connection',
    conn_id='snowflake_default',
    sql="SELECT CURRENT_TIMESTAMP;",
    dag=dag
)

# Web scrape data
scrape_task = PythonOperator(
    task_id='scrape_finance_data',
    python_callable=finance_data_scrapper,
    op_kwargs={
            'url': 'https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets',
            'bucket_name': 'bigdatasystems2',
            'req_folder': '2016q4'
        },
    #provide_context=True,
    dag=dag
)

# Text to JSON Transformation
process_task = PythonOperator(
    task_id='process_to_json',
    python_callable=get_converted_files,
    op_kwargs={
            'bucket_name': 'bigdatasystems2',
            'foldername': '2016q4'
        },
    #provide_context=True,
    dag=dag
)


# Add a task to create table
create_table = SQLExecuteQueryOperator(
    task_id='create_snowflake_table',
    conn_id='snowflake_default',
    sql="""
    CREATE OR REPLACE TABLE sec_2016q4 (
        id int PRIMARY KEY IDENTITY(1,1),
        city string,
        country string,
        data variant,
        enddate date,
        name string,
        secquarter string,
        startdate date,
        symbol string,
        secyear int
    );
    """,
    dag=dag
)


# Create stage
create_stage = SQLExecuteQueryOperator(
    task_id='create_s3_stage',
    conn_id='snowflake_default',
    sql="""
    CREATE STAGE IF NOT EXISTS my_s3_stage
    URL='s3://bigdatasystems2/exportFiles/2016q4/'
    CREDENTIALS=(AWS_KEY_ID='{{ conn.aws_default.login }}'
                AWS_SECRET_KEY='{{ conn.aws_default.password }}');
    """,
    dag=dag
)



# Snowflake Load task
load_to_snowflake = SQLExecuteQueryOperator(
    task_id='load_to_snowflake',
    conn_id='snowflake_default',
    sql="""
    COPY INTO sec_2016q4 (city, country, data, enddate, name, secquarter, startdate, symbol, secyear)
    FROM (
        SELECT 
            $1:city::string as city,
            $1:country::string as country,
            $1:data as data,
            TRY_TO_DATE($1:endDate::string, 'YYYY-MM-DD') as enddate,
            $1:name::string as name,
            $1:quarter::string as secquarter,
            TRY_TO_DATE($1:startDate::string, 'YYYY-MM-DD') as startdate,
            $1:symbol::string as symbol,
            $1:year::int as secyear
        FROM @my_s3_stage
        )
    FILE_FORMAT = (TYPE = JSON)
    PATTERN = '.*\.json';
    """,
    dag=dag
)






dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command= f'cd {dbt_path} && dbt debug --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_main_table = BashOperator(
    task_id='dbt_main_table',
    bash_command= f'cd {dbt_path} && dbt run --models sec_2016q4_main.sql --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_bs_table = BashOperator(
    task_id='dbt_bs_table',
    bash_command= f'cd {dbt_path} && dbt run --models sec_2016q4_bs.sql --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_cf_table = BashOperator(
    task_id='dbt_cf_table',
    bash_command= f'cd {dbt_path} && dbt run --models sec_2016q4_cf.sql --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_ic_table = BashOperator(
    task_id='dbt_ic_table',
    bash_command= f'cd {dbt_path} && dbt run --model sec_2016q4_ic.sql --profiles-dir /opt/dbt/.dbt',
    dag=dag
)




# Set task dependencies
test_conn_aws >> test_conn_snowflake >> scrape_task >> process_task >> create_table >> create_stage >> load_to_snowflake >> dbt_debug >> dbt_main_table >> [dbt_bs_table, dbt_cf_table, dbt_ic_table]
