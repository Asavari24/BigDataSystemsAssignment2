from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
#from test import finance_data_scrapper
#from dags.test import finance_data_scrapper
from test import finance_data_scrapper
#from transform_sec_data import list_extracted_files
#from transform_sec_data import process_all_files
from transform_sec_data import process_file


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
        bucket_name = 'assignment2bigdatasystemss'
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
            'bucket_name': 'assignment2bigdatasystemss',
            'req_folder': '2016q4'
        },
    #provide_context=True,
    dag=dag
)

'''# Transform SEC data from text to CSV
transform_task = PythonOperator(
    task_id='transform_sec_data',
    python_callable=process_all_files,  # Correct function
    dag=dag
)'''

from transform_sec_data import process_file

# Process only the '2016q4/' file from S3
transform_task = PythonOperator(
    task_id='transform_sec_data',
    python_callable=process_file,  # Process only one file
    op_kwargs={
        'bucket_name': 'assignment2bigdatasystemss',
        's3_key': 'importFiles/2016q4/'  # Update with the correct file path
        
    },
    dag=dag
)




# Add a task to create table
create_table = SQLExecuteQueryOperator(
    task_id='create_snowflake_table',
    conn_id='snowflake_default',
    sql="""
    CREATE OR REPLACE TABLE sec_2016q4 (
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
    URL='s3://assignment2bigdatasystemss/sec-transformed-files/2016q4/'
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
    FROM @my_s3_stage/2016q4/
        
    FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1)
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)


# Set task dependencies
#test_conn_aws >> test_conn_snowflake >> scrape_task >> process_task >> create_table >> create_stage >> load_to_snowflake
test_conn_aws >> test_conn_snowflake >> scrape_task >> transform_task >> create_table >> create_stage >> load_to_snowflake
