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

st.sidebar.title("Navigation")

page = st.sidebar.radio("Select Report", ["Company Details Report", "Balance Sheet & Cash Flow Graph"])


if page == "Company Details Report":

    st.header("Company Details for Financial Year")

    st.divider()

    # Input fields for SEC data
    year = st.selectbox("Select Year", [2009, 2016])
    quarter = st.slider("Select Quarter", 1, 4, 1)

    if st.button(f"SEC Data: {year} Q{quarter} Preview"):
        sec_data = call_fastapi("fetch_sec_data", {"fiscal_year": year, "quarter": quarter, "statement_type": "main"})
        if sec_data:
            st.table(sec_data[0:5])

            df = pd.DataFrame(sec_data)
            csv_data = df.to_csv(index=False).encode('utf-8')

            st.download_button(
                label=f"Download SEC Data: {year} Q{quarter}",
                data=csv_data,
                file_name="sec_data.csv",
                mime="text/csv"
            )

elif page == "Balance Sheet & Cash Flow Graph":

    st.header(f"Graph for Sum of Value(USD) for Concept")

    st.divider()

    company =  st.text_input("Enter Company Name")
    year = st.selectbox("Select Year", [2009, 2016])
    quarter = st.slider("Select Quarter", 1, 4, 1)

    st.divider()

    statement_type = st.selectbox("Select Financial Statement", ["Balance Sheet", "Cash Flow"])


    if statement_type == 'Balance Sheet':
        statement_type = 'bs'
    elif statement_type == "Cash Flow":
        statement_type = 'cf'


    if st.button(f"Bar Graph showing Sum of Values"):
        graph_data = call_fastapi("fetch_graph", {"fiscal_year": year, "quarter": quarter, "company": company, "statement_type": statement_type})
        if graph_data:
            st.bar_chart(graph_data,
                            x='CONCEPT', y='SUM(VALUE)',
                            x_label='Concept',
                            y_label='Sum of Value (USD)'
                        )



