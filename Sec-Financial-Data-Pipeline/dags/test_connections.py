from airflow.hooks.base import BaseHook
import boto3
from snowflake.connector import connect

def test_connections():
    """Test AWS and Snowflake connections before running the pipeline"""
    try:
        # Test AWS Connection
        aws_conn = BaseHook.get_connection('aws_default')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get('region_name')
        )
        buckets = s3_client.list_buckets()
        print("AWS Connection Successful!")
        print(f"Available buckets: {[bucket['Name'] for bucket in buckets['Buckets']]}")

        # Test Snowflake Connection
        snow_conn = BaseHook.get_connection('snowflake_default')
        extra = snow_conn.extra_dejson
        conn = connect(
            user=snow_conn.login,
            password=snow_conn.password,
            account=extra.get('account'),
            warehouse=extra.get('warehouse'),
            database=extra.get('database'),
            schema=extra.get('schema'),
            role=extra.get('role')
        )
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print("Snowflake Connection Successful!")
        print(f"Snowflake Version: {version}")
        cursor.close()
        conn.close()

        return True
    except Exception as e:
        print(f"Connection Test Failed: {str(e)}")
        raise