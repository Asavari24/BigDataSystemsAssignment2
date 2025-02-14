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

#@app.get("/calculate_profit_loss")
#async def calculate_profit_loss(fiscal_year: int = Query(None), quarter: int = Query(None, ge=1, le=4)):
    #"""Calculate profit, loss, and net income based on assets and liabilities for a given fiscal year and quarter."""
    #try:
        #conn = get_snowflake_connection()
        #cur = conn.cursor()
        
        #query = "SELECT * FROM dbt_bs_table"
        #filters = []
        
        #if fiscal_year:
            #filters.append(f"secyear = {fiscal_year}")
        
        #if quarter:
            #filters.append(f"secquarter = {quarter}")
        
        #if filters:
            #query += " WHERE " + " AND ".join(filters)
        
        #cur.execute(query)
        #records = cur.fetchall()
        
        #assets = {r[1]: r[6] for r in records if 'ASSET' in r[2]}
        #liabilities = {r[1]: r[6] for r in records if 'LIABILITY' in r[2]}
        
        #results = []
        #for main_id in assets.keys():
           # profit = assets.get(main_id, 0) + liabilities.get(main_id, 0)
            #loss = assets.get(main_id, 0) - liabilities.get(main_id, 0)
            #net_income = profit - loss
            #results.append({"main_id": main_id, "profit": profit, "loss": loss, "net_income": net_income})
        
        #cur.close()
        #conn.close()
        #return results
    #except Exception as e:
        #raise HTTPException(status_code=500, detail=str(e))


#@app.get("/export_data")
#async def export_data(fiscal_year: int = Query(..., ge=2009, le=2024), quarter: int = Query(..., ge=1, le=4)):
#    """Export financial data for a given fiscal year and quarter as an Excel file."""
#    try:
#        conn = get_snowflake_connection()
#        cur = conn.cursor()
#        query = f"""
#        SELECT * FROM dbt_main_table
#        WHERE secyear = {fiscal_year} AND secquarter = {quarter}
#        """
#        cur.execute(query)
#        records = cur.fetchall()
#        columns = [desc[0] for desc in cur.description]
#        df = pd.DataFrame(records, columns=columns)
#        cur.close()
#        conn.close()
#        file_path = "/tmp/financial_data.xlsx"
#        df.to_excel(file_path, index=False)
#        return {"message": "Data exported successfully", "file_path": file_path}
#    except Exception as e:
#        raise HTTPException(status_code=500, detail=str(e))


