from fastapi import FastAPI, HTTPException
from typing import Optional
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
from pydantic import BaseModel
from google.cloud import secretmanager
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = FastAPI()

def get_secret(secret_id):
    """Get secret from GCP Secret Manager"""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/finance-data-pipeline/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error fetching secret {secret_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve {secret_id} from Secret Manager"
        )

# Get Snowflake credentials with error handling
SNOWFLAKE_CONFIG = {
    "user": get_secret("SNOWFLAKE_USER"),
    "password": get_secret("SNOWFLAKE_PASSWORD"),
    "account": get_secret("SNOWFLAKE_ACCOUNT"),
    "warehouse": get_secret("SNOWFLAKE_WAREHOUSE"),
    "database": get_secret("SNOWFLAKE_DATABASE"),
    "role": get_secret("SNOWFLAKE_ROLE")
}

class QueryRequest(BaseModel):
    query: str
    schema: str

@app.post("/api/execute-query")
async def execute_query(request: QueryRequest):
    conn = None
    try:
        # Log the incoming request
        logger.info(f"Received query request - Schema: {request.schema}, Query: {request.query}")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        
        # Set the schema
        logger.info(f"Setting schema to: {request.schema}")
        cur.execute(f"USE SCHEMA {request.schema}")
        
        # Execute query
        logger.info(f"Executing query: {request.query}")
        cur.execute(request.query)
        
        # Fetch results
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Query executed successfully. Returned {len(data)} rows")
        return {"data": data}
        
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"Snowflake query error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Snowflake query error: {str(e)}")
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

@app.get("/")
async def root():
    return {
        "message": "Welcome to SEC Financial Data Pipeline API",
        "docs": "/docs",
        "health": "/health",
        "api": "/api/execute-query"
    }

@app.get("/debug-secrets")
async def debug_secrets():
    try:
        # Try to get each secret (without exposing values)
        secrets_status = {
            "SNOWFLAKE_USER": get_secret("SNOWFLAKE_USER") is not None,
            "SNOWFLAKE_ACCOUNT": get_secret("SNOWFLAKE_ACCOUNT") is not None,
            "SNOWFLAKE_WAREHOUSE": get_secret("SNOWFLAKE_WAREHOUSE") is not None,
            "SNOWFLAKE_DATABASE": get_secret("SNOWFLAKE_DATABASE") is not None,
            "SNOWFLAKE_ROLE": get_secret("SNOWFLAKE_ROLE") is not None,
            "SNOWFLAKE_PASSWORD": get_secret("SNOWFLAKE_PASSWORD") is not None
        }
        return {"secrets_status": secrets_status}
    except Exception as e:
        return {"error": str(e)}

