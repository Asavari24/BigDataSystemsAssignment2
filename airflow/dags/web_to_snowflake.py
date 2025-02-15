
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from web_scrapper_s3 import finance_data_scrapper
from text_to_json_converter import get_converted_files

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'web_to_snowflake',
    default_args=default_args,
    description='Scrape web data and load to Snowflake',
    schedule_interval='@daily',
    catchup=False
)


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


# Create file format
#create_file_format = SQLExecuteQueryOperator(
#    task_id='create_file_format',
#    conn_id='snowflake_default',
#    sql="""
#    CREATE OR REPLACE FILE FORMAT my_json_format
#        TYPE = 'JSON'
#        STRIP_OUTER_ARRAY = TRUE
#        COMPRESSION = GZIP;
#    """,
#    dag=dag
#)


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


# Set task dependencies
test_conn_aws >> test_conn_snowflake >> scrape_task >> process_task >> create_table >> create_stage >> load_to_snowflake
