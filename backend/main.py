from fastapi import FastAPI, HTTPException
from typing import Optional
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
from pydantic import BaseModel
import logging
from fastapi.middleware.cors import CORSMiddleware

# Set up logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "user": "dev_dhrumil_user",
    "password": "Dhrumil@123",  # Make sure this is your current password
    "account": "tr28614.us-east4.gcp",  # Full account identifier including region
    "warehouse": "Compute_WH",
    "database": "ASSIGNMENT2_TEAM1",
    "role": "ACCOUNTADMIN"
}

class QueryRequest(BaseModel):
    query: str
    schema: str

@app.post("/api/execute-query")
async def execute_query(request: QueryRequest):
    conn = None
    try:
        # Log connection attempt
        logger.info(f"Attempting to connect to Snowflake with config: {SNOWFLAKE_CONFIG}")
        
        # Create connection
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        
        # Set and verify database context
        logger.info(f"Setting database context to: {SNOWFLAKE_CONFIG['database']}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cur.execute("SELECT CURRENT_DATABASE()")
        current_db = cur.fetchone()[0]
        logger.info(f"Current database is: {current_db}")
        
        # Set and verify schema context
        logger.info(f"Setting schema context to: {request.schema}")
        cur.execute(f"USE SCHEMA {request.schema}")
        cur.execute("SELECT CURRENT_SCHEMA()")
        current_schema = cur.fetchone()[0]
        logger.info(f"Current schema is: {current_schema}")
        
        # List available tables
        logger.info("Listing available tables:")
        cur.execute(f"SHOW TABLES IN SCHEMA {current_db}.{current_schema}")
        tables = cur.fetchall()
        for table in tables:
            logger.info(f"Found table: {table[1]}")
        
        # Execute the query
        logger.info(f"Executing query: {request.query}")
        cur.execute(request.query)
        
        # Fetch results
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        # Convert to list of dicts
        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Query executed successfully. Returned {len(data)} rows")
        
        return {"data": data}
        
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        error_context = {
            "detail": str(e),
            "database": SNOWFLAKE_CONFIG['database'],
            "schema": request.schema,
            "query": request.query,
            "error": str(e)
        }
        raise HTTPException(status_code=500, detail=error_context)
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Connection closed")
            except:
                pass

@app.get("/health")
async def health_check():
    try:
        # Test Snowflake connection
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        conn.close()
        return {
            "status": "healthy",
            "snowflake_connected": True,
            "snowflake_version": version
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "snowflake_connected": False,
            "error": str(e)
        }

@app.get("/debug-info")
async def debug_info():
    conn = None
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        
        # Get current context
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        context = cur.fetchone()
        
        # List all databases
        cur.execute("SHOW DATABASES")
        databases = [row[1] for row in cur.fetchall()]
        
        # Use the correct database
        cur.execute("USE DATABASE ASSIGNMENT2_TEAM1")
        
        # List all schemas
        cur.execute("SHOW SCHEMAS")
        schemas = [row[1] for row in cur.fetchall()]
        
        # Check RAW_STAGING tables
        cur.execute("USE SCHEMA RAW_STAGING")
        cur.execute("SHOW TABLES")
        raw_tables = [row[1] for row in cur.fetchall()]
        
        return {
            "current_context": {
                "database": context[0],
                "schema": context[1],
                "role": context[2],
                "warehouse": context[3]
            },
            "databases": databases,
            "schemas": schemas,
            "raw_staging_tables": raw_tables
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            conn.close() 