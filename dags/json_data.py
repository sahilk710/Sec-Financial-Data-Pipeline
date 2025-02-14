import json
import boto3
from snowflake.connector import connect

def read_json_from_s3():
    """Read JSON data from S3"""
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Your S3 bucket and file details
        bucket_name = 'your-bucket-name'
        file_key = 'path/to/your/json/file.json'
        
        print(f"Reading JSON from S3: {bucket_name}/{file_key}")
        
        # Get object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        
        return json_data
        
    except Exception as e:
        print(f"Error reading JSON from S3: {str(e)}")
        raise

def load_json_to_snowflake(json_data):
    """Load JSON data to Snowflake"""
    try:
        # Snowflake connection parameters
        conn = connect(
            user='dev_dhrumil_user',
            password='Dhrumil@123',
            account='tr28614.us-east4.gcp',
            warehouse='COMPUTE_WH',
            database='assignment2_team1',
            schema='raw_staging'
        )
        
        print("Connected to Snowflake")
        cursor = conn.cursor()
        
        # Your JSON loading logic here
        print("Loading JSON data to Snowflake")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error loading JSON to Snowflake: {str(e)}")
        raise

def verify_data_load():
    """Verify data was loaded correctly"""
    try:
        conn = connect(
            user='dev_dhrumil_user',
            password='Dhrumil@123',
            account='tr28614.us-east4.gcp',
            warehouse='COMPUTE_WH',
            database='assignment2_team1',
            schema='raw_staging'
        )
        
        cursor = conn.cursor()
        
        # Add verification queries here
        print("Verifying data load")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error verifying data load: {str(e)}")
        raise