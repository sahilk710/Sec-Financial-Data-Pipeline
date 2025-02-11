import boto3
import json
import snowflake.connector
import logging
from datetime import datetime
import os
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS Configuration
AWS_CONFIG = {
    'region_name': 'us-east-1',
    'bucket_name': 'sec-finance-data-team1',
    'prefix': 'sec_data/2024q4/json/'  # Path to JSON files
}

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    'account': 'tr28614.us-east4.gcp',
    'user': 'dev_dhrumil_user',
    'password': 'Dhrumil@123',
    'warehouse': 'COMPUTE_WH',
    'database': 'assignment2_team1',
    'schema': 'json_staging',
    'client_session_keep_alive': True,
    'client_prefetch_threads': 4,
    'client_memory_limit': '4096'
}

def get_s3_client():
    """Create and return S3 client"""
    return boto3.client('s3', region_name=AWS_CONFIG['region_name'])

def read_json_from_s3():
    """Read JSON files from S3"""
    try:
        s3_client = get_s3_client()
        json_data = []
        
        response = s3_client.list_objects_v2(
            Bucket=AWS_CONFIG['bucket_name'],
            Prefix=AWS_CONFIG['prefix']
        )
        
        logger.info("Files found in S3:")
        for obj in response.get('Contents', []):
            logger.info(f"Found file: {obj['Key']}")
            
            if obj['Key'].endswith('.json'):
                try:
                    file_response = s3_client.get_object(
                        Bucket=AWS_CONFIG['bucket_name'],
                        Key=obj['Key']
                    )
                    
                    content = json.loads(file_response['Body'].read().decode('utf-8'))
                    json_data.append({
                        'content': content,
                        'filename': obj['Key'].split('/')[-1],
                        'load_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    logger.info(f"Successfully processed {obj['Key']}")
                    
                except Exception as e:
                    logger.error(f"Error processing {obj['Key']}: {str(e)}")
                    continue
        
        return json_data
        
    except Exception as e:
        logger.error(f"Error reading from S3: {str(e)}")
        raise

def setup_snowflake_warehouse():
    """Set up Snowflake warehouse with required settings"""
    conn = None
    cursor = None
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        
        # Resume and resize warehouse
        commands = [
            "ALTER WAREHOUSE COMPUTE_WH RESUME IF SUSPENDED",
            "ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XLARGE'",
            "ALTER WAREHOUSE COMPUTE_WH SET MAX_CONCURRENCY_LEVEL = 8",
            "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 7200",  # 2 hours
            "ALTER SESSION SET LOCK_TIMEOUT = 3600",  # 1 hour
            "ALTER WAREHOUSE COMPUTE_WH SET STATEMENT_TIMEOUT_IN_SECONDS = 7200"
        ]
        
        for command in commands:
            try:
                cursor.execute(command)
                logger.info(f"Successfully executed: {command}")
            except Exception as e:
                logger.error(f"Error executing {command}: {str(e)}")
                raise
                
        return conn, cursor
        
    except Exception as e:
        logger.error(f"Error setting up warehouse: {str(e)}")
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        raise

def reset_warehouse(cursor):
    """Reset warehouse to original settings"""
    try:
        commands = [
            "ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL'",
            "ALTER WAREHOUSE COMPUTE_WH SET MAX_CONCURRENCY_LEVEL = 1",
            "ALTER WAREHOUSE COMPUTE_WH SUSPEND"
        ]
        
        for command in commands:
            try:
                cursor.execute(command)
                logger.info(f"Successfully executed: {command}")
            except Exception as e:
                logger.error(f"Error executing {command}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error resetting warehouse: {str(e)}")


def process_json_chunk(content, max_rows=100):
    """Process JSON content in smaller chunks and handle NaN values"""
    if isinstance(content, list):
        for i in range(0, len(content), max_rows):
            chunk = content[i:i + max_rows]
            # Handle NaN values
            processed_chunk = []
            for item in chunk:
                processed_item = {}
                for key, value in item.items():
                    if isinstance(value, float) and str(value).lower() == 'nan':
                        processed_item[key] = None
                    else:
                        processed_item[key] = value
                processed_chunk.append(processed_item)
            yield processed_chunk, i // max_rows
    else:
        yield content, 0

def create_staging_table(cursor):
    """Create staging table for JSON data"""
    try:
        # Drop the table if it exists
        cursor.execute("DROP TABLE IF EXISTS raw_json")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_json (
            id NUMBER AUTOINCREMENT,
            filename VARCHAR,
            chunk_id NUMBER,
            raw_json VARIANT,
            load_timestamp TIMESTAMP_NTZ,
            file_row_number NUMBER
        );
        """
        cursor.execute(create_table_query)
        logger.info("Successfully created staging table")
        
    except Exception as e:
        logger.error(f"Error creating staging table: {str(e)}")
        raise


        yield content, 0

def load_json_to_snowflake(json_data):
    """Load JSON data to Snowflake"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        
        # Set warehouse to XLARGE
        cursor.execute("ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XLARGE'")
        
        # Create table
        create_table_query = """
        CREATE OR REPLACE TABLE raw_json (
            id NUMBER AUTOINCREMENT,
            filename STRING,
            chunk_id NUMBER,
            raw_json VARIANT,
            load_timestamp TIMESTAMP_NTZ
        );
        """
        cursor.execute(create_table_query)
        
        # Process each file
        for data in json_data:
            filename = data['filename']
            content = data['content']
            load_timestamp = data['load_timestamp']
            
            logger.info(f"Processing file: {filename}")
            
            # Process in chunks
            for chunk, chunk_id in process_json_chunk(content, max_rows=100):
                try:
                    # Convert chunk to JSON string
                    chunk_json = json.dumps(chunk)
                    
                    # Insert using PARSE_JSON
                    insert_query = """
                    INSERT INTO raw_json (filename, chunk_id, raw_json, load_timestamp)
                    SELECT 
                        %s,
                        %s,
                        PARSE_JSON(%s),
                        %s
                    """
                    
                    cursor.execute(insert_query, (
                        filename,
                        chunk_id,
                        chunk_json,
                        load_timestamp
                    ))
                    
                    conn.commit()
                    logger.info(f"Loaded chunk {chunk_id} for {filename}")
                    
                except Exception as e:
                    logger.error(f"Error loading chunk {chunk_id} of {filename}: {str(e)}")
                    logger.error(f"First row of problematic chunk: {chunk[0] if chunk else 'Empty chunk'}")
                    conn.rollback()
                    continue
        
        logger.info("Successfully loaded all JSON data")
        
    except Exception as e:
        logger.error(f"Error in load process: {str(e)}")
        raise
    finally:
        try:
            # Reset warehouse size
            cursor.execute("ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL'")
        except:
            pass
        
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    """Load JSON data to Snowflake"""
    conn = None
    cursor = None
    try:
        # Setup warehouse
        conn, cursor = setup_snowflake_warehouse()
        
        # Create staging table
        create_staging_table(cursor)
        
        # Process each file
        for data in json_data:
            filename = data['filename']
            content = data['content']
            load_timestamp = data['load_timestamp']
            
            logger.info(f"Processing file: {filename}")
            
            # Process in chunks
            for chunk, chunk_id in process_json_chunk(content):
                try:
                    insert_query = """
                    INSERT INTO raw_json (filename, chunk_id, raw_json, load_timestamp, file_row_number)
                    VALUES (%s, %s, PARSE_JSON(%s), %s, %s)
                    """
                    
                    cursor.execute(insert_query, (
                        filename,
                        chunk_id,
                        json.dumps(chunk),
                        load_timestamp,
                        chunk_id * 1000
                    ))
                    
                    conn.commit()
                    logger.info(f"Loaded chunk {chunk_id} for {filename}")
                    
                except Exception as e:
                    logger.error(f"Error loading chunk {chunk_id} of {filename}: {str(e)}")
                    conn.rollback()
                    time.sleep(5)  # Wait before retrying
                    continue
            
        logger.info("Successfully loaded all JSON data")
        
    except Exception as e:
        logger.error(f"Error in load process: {str(e)}")
        raise
        
    finally:
        if cursor and conn:
            reset_warehouse(cursor)
            cursor.close()
        if conn:
            conn.close()
    """Load JSON data to Snowflake"""
    conn = None
    cursor = None
    try:
        # Setup warehouse
        conn, cursor = setup_snowflake_warehouse()
        
        # Create staging table
        create_staging_table(cursor)
        
        # Process each file
        for data in json_data:
            filename = data['filename']
            content = data['content']
            load_timestamp = data['load_timestamp']
            
            logger.info(f"Processing file: {filename}")
            
            # Process in chunks
            for chunk, chunk_id in process_json_chunk(content):
                try:
                    insert_query = """
                    INSERT INTO raw_json (filename, chunk_id, raw_json, load_timestamp, file_row_number)
                    SELECT %s, %s, PARSE_JSON(%s), %s, %s
                    """
                    
                    cursor.execute(insert_query, (
                        filename,
                        chunk_id,
                        json.dumps(chunk),
                        load_timestamp,
                        chunk_id * 1000
                    ))
                    
                    conn.commit()
                    logger.info(f"Loaded chunk {chunk_id} for {filename}")
                    
                except Exception as e:
                    logger.error(f"Error loading chunk {chunk_id} of {filename}: {str(e)}")
                    conn.rollback()
                    time.sleep(5)  # Wait before retrying
                    continue
            
        logger.info("Successfully loaded all JSON data")
        
    except Exception as e:
        logger.error(f"Error in load process: {str(e)}")
        raise
        
    finally:
        if cursor and conn:
            reset_warehouse(cursor)
            cursor.close()
        if conn:
            conn.close()

def verify_data_load():
    """Verify the data load"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        
        queries = [
            """
            SELECT filename, 
                   COUNT(*) as chunks,
                   MIN(load_timestamp) as first_loaded,
                   MAX(load_timestamp) as last_loaded,
                   SUM(ARRAY_SIZE(raw_json)) as total_records
            FROM raw_json
            GROUP BY filename
            """
        ]
        
        for query in queries:
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info("\nLoad Summary:")
            for row in results:
                logger.info(f"File: {row[0]}")
                logger.info(f"  Chunks: {row[1]}")
                logger.info(f"  Load Time Range: {row[2]} to {row[3]}")
                logger.info(f"  Total Records: {row[4]}")
                
    except Exception as e:
        logger.error(f"Error verifying load: {str(e)}")
        raise
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    """Main execution function"""
    try:
        # Extract JSON data
        logger.info("Starting JSON data extraction from S3...")
        json_data = read_json_from_s3()
        
        # Load to Snowflake
        logger.info("Loading JSON data to Snowflake...")
        load_json_to_snowflake(json_data)
        
        # Verify load
        logger.info("Verifying data load...")
        verify_data_load()
        
        logger.info("Process completed successfully!")
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()