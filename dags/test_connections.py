import boto3
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

logger = logging.getLogger(__name__)

def test_connections():
    """Test AWS and Snowflake connections"""
    try:
        # Test AWS connection
        aws_conn = BaseHook.get_connection('aws_default')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name='us-east-1'
        )
        s3_client.list_buckets()
        logger.info("AWS connection successful")

        # Test Snowflake connection
        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        snow_hook.get_conn()
        logger.info("Snowflake connection successful")

        return True

    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise