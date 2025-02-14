from fastapi import FastAPI, HTTPException
from typing import Optional
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
from pydantic import BaseModel
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = FastAPI()

# Snowflake connection parameters for Airflow
AIRFLOW_SNOWFLAKE_CONFIG = {
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
    "database": os.getenv('SNOWFLAKE_DATABASE'),
    "schema": os.getenv('SNOWFLAKE_SCHEMA'),  # This will be raw_staging
    "role": os.getenv('SNOWFLAKE_ROLE')
}

# Snowflake connection parameters for API queries
API_SNOWFLAKE_CONFIG = {
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
    "database": os.getenv('SNOWFLAKE_DATABASE'),
    "role": os.getenv('SNOWFLAKE_ROLE')
}

class QueryRequest(BaseModel):
    query: str
    schema: str

@app.post("/api/execute-query")
async def execute_query(request: QueryRequest):
    conn = None
    try:
        # Use API config (without schema) for queries
        conn = snowflake.connector.connect(**API_SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        
        # Set the schema based on request
        cur.execute(f"USE SCHEMA {request.schema}")
        logger.info(f"Schema set to: {request.schema}")
        
        # Execute the query
        cur.execute(request.query)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        data = [dict(zip(columns, row)) for row in results]
        return {"data": data}
        
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

