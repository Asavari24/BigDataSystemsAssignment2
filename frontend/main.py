import os
import snowflake.connector
from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
import requests

load_dotenv(r'C:\Users\Admin\Desktop\MS Data Architecture and Management\DAMG 7245 - Big Data Systems and Intelligence Analytics\Assignment 2\environment\snowflake_access.env')

app = FastAPI()

# FastAPI endpoint for Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    

# Streamlit Integration
BASE_URL = "http://127.0.0.1:8000"  # Update with actual server URL if deployed

# Function to call FastAPI endpoints and handle response
def call_fastapi(endpoint, params=None):
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()  # Raise error for bad HTTP responses
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error while calling FastAPI: {e}")
        return None

# Streamlit UI layout
st.title("Financial Data Dashboard")



# Dropdown to select financial statement type
statement_type = st.selectbox("Select Financial Statement", ["balance_sheet", "income_statement", "cash_flow", "company_details"])

if statement_type == 'balance_sheet':
    statement_type = 'bs'
elif statement_type == "cash_flow":
    statement_type = 'cf'
elif statement_type == 'income_statement':
    statement_type = 'ic'
else:
    statement_type = 'main'

# Input fields for SEC data
year = st.number_input("Enter Year", min_value=2000, max_value=2030, value=2024, step=1)
quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])




if st.button("Fetch SEC Data"):
    sec_data = call_fastapi("fetch_sec_data", {"fiscal_year": year, "quarter": quarter, "statement_type": statement_type})
    if sec_data:
        st.table(sec_data)



#if st.button("Export Data to Excel"):
#    export_result = call_fastapi("export_data", {"fiscal_year": year, "quarter": quarter, "table_name": statement_type})
#    if export_result:
#        file_path = export_result["file_path"]
#        st.write(f"Data exported successfully! Download your file [here](file://{file_path})")

