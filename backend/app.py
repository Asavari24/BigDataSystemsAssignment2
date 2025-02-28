import os
from fastapi.responses import FileResponse
import snowflake.connector
from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import pandas as pd

load_dotenv(r'C:\Users\Admin\Desktop\MS Data Architecture and Management\DAMG 7245 - Big Data Systems and Intelligence Analytics\Assignment 2\environment\snowflake_access.env')

app = FastAPI()

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

@app.get("/fetch_sec_data")
async def fetch_sec_data(fiscal_year: int = Query(..., ge=2009, le=2024), quarter: int = Query(..., ge=1, le=4), statement_type: str = Query(...)):
    """Fetch financial statement data from SEC for a given fiscal year and quarter."""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        query = f"""
        SELECT * FROM sec_{fiscal_year}q{quarter}_{statement_type}
        """
        cur.execute(query)
        records = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(records, columns=columns)
        cur.close()
        conn.close()
        #return [dict(zip(columns, record)) for record in records]
        return df.to_dict(orient="records")  # Return results in tabular format
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fetch_graph")
async def fetch_graph(fiscal_year: int = Query(..., ge=2009, le=2024), quarter: int = Query(..., ge=1, le=4), company: str = Query(...), statement_type: str = Query(...)):
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        query = f"""
        SELECT CONCEPT, SUM(VALUE) FROM sec_{fiscal_year}q{quarter}_{statement_type}
        WHERE MAIN_ID = (SELECT MAIN_ID FROM sec_{fiscal_year}q{quarter}_main WHERE NAME LIKE '%{company}%')
        GROUP BY CONCEPT
        """
        cur.execute(query)
        records = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(records, columns=columns)
        cur.close()
        conn.close()

        return df.to_dict(orient="records")  # Return results in tabular format
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



