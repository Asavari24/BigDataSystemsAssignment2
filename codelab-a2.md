summary: SEC Financial Data Pipeline
id: sec-financial-data-pipeline
categories: Data Engineering, Cloud, AWS, Snowflake
status: Published
authors: Hishita Thakkar
feedback link: https://your-feedback-link.com

# SEC Financial Statement Data Pipeline Codelab

Welcome to this Codelab! In this tutorial, you'll learn how to build a **data pipeline** for scraping, transforming, and storing **SEC financial statement data** using **BeautifulSoup, Apache Airflow, Snowflake, DBT, FastAPI, and Streamlit**.

---

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Setup Instructions](#setup-instructions)
3. [Step 1: Scraping SEC Financial Statement Data](#step-1-scraping-sec-financial-statement-data)
4. [Step 2: Data Storage in S3](#step-2-data-storage-in-s3)
5. [Step 3: Building the Airflow Data Pipeline](#step-3-building-the-airflow-data-pipeline)
6. [Step 4: Loading Data into Snowflake](#step-4-loading-data-into-snowflake)
7. [Step 5: Performing DBT Transformations](#step-5-performing-dbt-transformations)
8. [Step 6: Connecting the Pipeline to FastAPI and Streamlit](#step-6-connecting-the-pipeline-to-fastapi-and-streamlit)
9. [Conclusion](#conclusion)

---

## Step 1: Scraping SEC Financial Statement Data

### **Using BeautifulSoup to Scrape SEC Data**
Create `scrape_sec.py`:
```python
import requests
from bs4 import BeautifulSoup
import zipfile
import os

def download_sec_zip():
    url = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)
    zip_links = [link['href'] for link in links if link['href'].endswith('.zip')]
    
    for zip_link in zip_links[:1]:  # Download only the latest zip file
        zip_url = f"https://www.sec.gov{zip_link}"
        zip_name = zip_url.split('/')[-1]
        
        with open(zip_name, 'wb') as f:
            f.write(requests.get(zip_url, headers=headers).content)

        print(f"Downloaded {zip_name}")

if __name__ == "__main__":
    download_sec_zip()
```

Run the script:
```bash
python scrape_sec.py
```

---

## Step 2: Data Storage in S3

### **Uploading Data to AWS S3**
Create `upload_s3.py`:
```python
import boto3

def upload_to_s3(file_name, bucket, folder):
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket, f"{folder}/{file_name}")
    print(f"Uploaded {file_name} to S3 {bucket}/{folder}")

if __name__ == "__main__":
    upload_to_s3("financial_data.zip", "your-bucket-name", "scraped_data")
```

Run the script:
```bash
python upload_s3.py
```

---

## Step 3: Building the Airflow Data Pipeline

Create `sec_data_pipeline.py`:
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scrape_sec import download_sec_zip
from upload_s3 import upload_to_s3

def run_scraping():
    download_sec_zip()
    upload_to_s3("financial_data.zip", "your-bucket-name", "scraped_data")

def run_snowflake_load():
    pass

def run_dbt_transform():
    pass

def run_final_integration():
    pass

default_args = {"start_date": datetime(2024, 1, 1)}

dag = DAG("sec_data_pipeline", default_args=default_args, schedule_interval="@daily")

scraping_task = PythonOperator(task_id="scrape_data", python_callable=run_scraping, dag=dag)
snowflake_task = PythonOperator(task_id="load_snowflake", python_callable=run_snowflake_load, dag=dag)
dbt_task = PythonOperator(task_id="dbt_transform", python_callable=run_dbt_transform, dag=dag)
final_task = PythonOperator(task_id="final_integration", python_callable=run_final_integration, dag=dag)

scraping_task >> snowflake_task >> dbt_task >> final_task
```

Run Airflow:
```bash
airflow dags list
airflow dags trigger sec_data_pipeline
```

---

## Step 4: Loading Data into Snowflake
```python
import snowflake.connector

def load_data_to_snowflake():
    conn = snowflake.connector.connect(
        user='your-user',
        password='your-password',
        account='your-account'
    )
    cur = conn.cursor()
    cur.execute("COPY INTO financial_statements FROM @s3_stage;")
    conn.close()
```

---

## Step 5: Performing DBT Transformations
```sql
SELECT * FROM financial_statements WHERE year = 2024;
```
Run DBT:
```bash
dbt run
```

---

## Step 6: Connecting to FastAPI & Streamlit

### **FastAPI Backend**
Create `fastapi_service.py`:
```python
from fastapi import FastAPI
app = FastAPI()

@app.get("/data")
def get_data():
    return {"message": "Financial Data Retrieved"}
```

### **Streamlit Frontend**
Create `streamlit_dashboard.py`:
```python
import streamlit as st
st.title("SEC Financial Data Dashboard")
st.write("Displaying financial data here...")
```
Run:
```bash
streamlit run app.py
```

---

## Conclusion
You have successfully built a **data pipeline** for scraping SEC financial data, transforming it with Airflow and DBT, storing it in Snowflake, and integrating it with FastAPI and Streamlit!

Happy coding! 🚀

---

## Additional Resources
- [SEC Data Repository](https://www.sec.gov/data)
- [AWS S3 Docs](https://docs.aws.amazon.com/s3/)
- [Snowflake Docs](https://docs.snowflake.com/)
- [Apache Airflow](https://airflow.apache.org/)
- [DBT](https://docs.getdbt.com/)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Streamlit](https://docs.streamlit.io/)
