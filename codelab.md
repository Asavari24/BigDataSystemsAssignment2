

summary: Automated SEC Financial Data Extraction & Transformation System
id: sec-financial-data-pipeline
categories: Data Engineering, Cloud, AWS
status: Published
authors: Hishita Thakkar
feedback link: https://github.com/Asavari24/BigDataSystemsAssignment2/issues

## Overview

This codelab will walk you through the implementation of an automated SEC Financial Data Extraction & Transformation System. By the end, you will have a fully functional data pipeline that scrapes, transforms, and stores SEC financial data for querying and analysis.

### What you'll learn
- Scrape SEC financial datasets using Python & BeautifulSoup.
- Store and transform financial data in S3 and Snowflake.
- Automate pipeline execution using Apache Airflow.
- Use DBT for schema validation and data transformation.
- Develop a FastAPI backend and a Streamlit UI for visualization.

### What you'll need
- Basic knowledge of Python, SQL, and cloud computing.
- An AWS account with S3 and IAM setup.
- A Snowflake account for data storage.
- Apache Airflow and DBT installed.

## Step 1: Set Up Your Environment

### 1.1 Clone the Repository
```sh
sudo su
source venv/bin/activate
git clone https://github.com/Asavari24/BigDataSystemsAssignment2.git
cd BigDataSystemsAssignment2
```

### 1.2 Install Dependencies
```sh
pip install -r requirements.txt
```

## Step 2: Data Extraction & Storage

### 2.1 Scrape SEC Financial Data
- Use BeautifulSoup to download ZIP files from the SEC website.
- Extract and store the data in an S3 bucket.

```python
from bs4 import BeautifulSoup
import requests
import boto3

# Fetch SEC data
url = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Upload to S3
s3 = boto3.client('s3')
s3.upload_file("financial_data.zip", "big.data.ass3", "scraped/financial_data.zip")
```

## Step 3: Data Transformation with DBT

### 3.1 Define DBT Models
Create DBT models for staging and transformation.

```sql
-- models/staging/sub.sql
SELECT * FROM raw_data.sec_sub;
```

```sql
-- models/transformations/financial_data.sql
SELECT cik, ticker, filing_date, revenue FROM staging.sub;
```

## Step 4: Automate Pipeline Execution with Apache Airflow

### 4.1 Define an Airflow DAG

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def extract_and_upload():
    # Extraction logic
    pass

def transform_data():
    # DBT transformations
    pass

def load_to_snowflake():
    # Snowflake load logic
    pass

define_dag = DAG(
    'sec_data_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 3, 1),
    catchup=False
)

extract_task = PythonOperator(task_id='extract', python_callable=extract_and_upload, dag=define_dag)
transform_task = PythonOperator(task_id='transform', python_callable=transform_data, dag=define_dag)
load_task = PythonOperator(task_id='load', python_callable=load_to_snowflake, dag=define_dag)

extract_task >> transform_task >> load_task
```

## Step 5: Deploy FastAPI and Streamlit UI

### 5.1 Start FastAPI
```sh
uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
```

### 5.2 Start Streamlit UI
```sh
streamlit run frontend/main.py
```

## Step 6: Access the Application

### 6.1 Streamlit UI
Access: [Streamlit UI](http://3.130.104.76:8501/)

### 6.2 FastAPI Docs
Access: [FastAPI Docs](http://3.130.104.76:8000/docs)

## Conclusion

Congratulations! 🎉 You have successfully built an automated SEC Financial Data Extraction & Transformation System using Apache Airflow, DBT, Snowflake, FastAPI, and Streamlit.

### Next Steps
✅ Optimize JSON transformations for better structuring.
✅ Improve Airflow scheduling and execution speed.
✅ Deploy in a live financial analysis environment.
✅ Assess cost efficiency of different storage methods.

## Resources
- [SEC Financial Statement Data Sets](https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets)
- [DBT Documentation](https://docs.getdbt.com/)
- [Apache Airflow Documentation](https://airflow.apache.org/)

