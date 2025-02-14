from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import io
import os
import zipfile
import requests

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'sec_financial_data_pipeline',
    default_args=default_args,
    description='Scrape SEC financial data for Q4 2016 and load into Snowflake',
    schedule_interval=None,  # Run manually
    catchup=False
)

# AWS S3 configuration
AWS_BUCKET_NAME = 'assignment2bigdatasystems'
AWS_CONN_ID = 'aws_default'
S3_FOLDER = 'extracted/'
SEC_BASE_URL = "https://www.sec.gov"

# Snowflake configuration
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_DATABASE = 'ASSIGNMENT'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_STAGE = 'sec_stage'
SNOWFLAKE_TABLE = 'financial_data'


def upload_file_to_s3(file_content, s3_path):
    """Uploads a file content to S3."""
    try:
        file_buffer = io.BytesIO(file_content)
        s3_hook = S3Hook(aws_conn_id='aws_default')
        print(f"Uploading to S3: {s3_path}")
        s3_hook.get_conn().upload_fileobj(file_buffer, 'assignment2bigdatasystems', s3_path)
        print(f"Uploaded successfully: {s3_path}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")


def fetch_and_upload_q4_2016_zip(source_url, bucket_name):
    """Fetches the SEC page, downloads Q4 2016 ZIP, extracts and uploads its contents to S3."""
    print(f"Fetching data from {source_url}...")  
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0'
    }

    try:
        response = requests.get(source_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch webpage: {response.status_code}")
            return
        
        page_content = BeautifulSoup(response.content, 'html.parser')
        
        for anchor in page_content.find_all('a'):
            file_link = anchor.get('href')
            if file_link and "2016q4.zip" in file_link.lower():
                zip_url = requests.compat.urljoin(SEC_BASE_URL, file_link)
                print(f"Downloading ZIP file: {zip_url}")

                zip_response = requests.get(zip_url, headers=headers)
                if zip_response.status_code == 200:
                    zip_content = io.BytesIO(zip_response.content)
                    print("Extracting ZIP contents...")
                    
                    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
                        for file_name in zip_ref.namelist():
                            with zip_ref.open(file_name) as extracted_file:
                                file_data = extracted_file.read()
                                s3_file_path = f"extracted/2016q4/{file_name.replace('.csv', '.txt')}" 
                                upload_file_to_s3(file_data, s3_file_path)

                    print("All files extracted and uploaded successfully.")
                else:
                    print(f"Failed to download ZIP: {zip_url}, Status: {zip_response.status_code}")

                return  # Stop after processing the required file

        print("q4_2016.zip not found.")

    except Exception as e:
        print(f"Error while fetching data: {e}")


fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload_q4_2016_zip',
    python_callable=fetch_and_upload_q4_2016_zip,
    op_kwargs={
        'source_url': 'https://www.sec.gov/dera/data/financial-statement-data-sets.html',
        'bucket_name': 'assignment2bigdatasystems'
    },
    dag=dag,
)


# Snowflake Task: Create External Stage
create_s3_stage = SnowflakeOperator(
    task_id='create_s3_stage',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    CREATE STAGE IF NOT EXISTS {SNOWFLAKE_STAGE}
    URL='s3://{AWS_BUCKET_NAME}/{S3_FOLDER}'
    CREDENTIALS=(AWS_KEY_ID='{{{{ conn.aws_default.login }}}}'
                 AWS_SECRET_KEY='{{{{ conn.aws_default.password }}}}');
    """,
    dag=dag
)

# Snowflake Task: Create Table
create_table = SnowflakeOperator(
    task_id='create_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        CIK VARCHAR,
        Name VARCHAR,
        SIC VARCHAR,
        FYEAR INT,
        Period VARCHAR,
        Form VARCHAR,
        Filed_Date DATE,
        URL VARCHAR
    );
    """,
    dag=dag
)

# Snowflake Task: Create File Format
create_file_format = SnowflakeOperator(
    task_id='create_file_format',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    CREATE OR REPLACE FILE FORMAT {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.sec_txt_format
        TYPE = 'CSV'
        FIELD_DELIMITER = '|'
        SKIP_HEADER = 1
        NULL_IF = ('NULL', 'null')
        EMPTY_FIELD_AS_NULL = TRUE;
    """,
    dag=dag
)

# Snowflake Task: Load Data from S3
load_to_snowflake = SnowflakeOperator(
    task_id='load_to_snowflake',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    COPY INTO ASSIGNMENT.PUBLIC.FINANCIAL_DATA
    FROM @ASSIGNMENT.PUBLIC.SEC_STAGE
    FILE_FORMAT = ASSIGNMENT.PUBLIC.SEC_TXT_FORMAT
    PATTERN = '.*.txt'
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)


# **Define DAG Task Dependencies**
fetch_and_upload_task >> create_s3_stage >> create_file_format >> create_table >> load_to_snowflake
