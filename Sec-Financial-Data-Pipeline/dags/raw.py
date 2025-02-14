import requests
from bs4 import BeautifulSoup
import os
import boto3
import time
import pandas as pd
from zipfile import ZipFile
import logging
import urllib3
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import shutil

logger = logging.getLogger(__name__)

def download_sec_data(year, quarter):
    """Download SEC financial statement data sets"""
    try:
        # Direct download URL
        download_url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
        
        headers = {
            'User-Agent': 'Sample Company Name AdminContact@company.com',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        
        logger.info(f"Attempting to download from: {download_url}")
        
        # Create directory
        os.makedirs("./data/financial_statement_data_sets", exist_ok=True)
        
        # Download file
        local_filename = f"./data/financial_statement_data_sets/{year}q{quarter}.zip"
        
        # Add delay to respect SEC rate limits
        time.sleep(0.1)
        
        response = requests.get(
            download_url, 
            headers=headers,
            allow_redirects=True
        )
        
        logger.info(f"Download status code: {response.status_code}")
        
        if response.status_code == 200:
            with open(local_filename, 'wb') as f:
                f.write(response.content)
            logger.info(f"Successfully downloaded {year}q{quarter}.zip")
            return [[local_filename, f"{year}q{quarter}.zip"]]
        else:
            raise ValueError(f"Download failed with status code: {response.status_code}")
            
    except Exception as e:
        logger.error(f"Error downloading SEC data: {str(e)}")
        raise
    """Download SEC financial statement data sets"""
    try:
        base_url = "https://www.sec.gov/dera/data/financial-statement-data-sets.html"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }

        # First get the main page
        logger.info(f"Fetching main page: {base_url}")
        response = requests.get(base_url, headers=headers)
        logger.info(f"Main page status code: {response.status_code}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all links
        links = soup.find_all('a')
        logger.info(f"Found {len(links)} links on the page")
        
        target_filename = f"{year}q{quarter}.zip"
        logger.info(f"Looking for file: {target_filename}")
        
        download_url = None
        for link in links:
            href = link.get('href', '')
            logger.info(f"Checking link: {href}")
            if target_filename in href:
                download_url = f"https://www.sec.gov{href}"
                break
        
        if not download_url:
            logger.error(f"Could not find link for {target_filename}")
            raise ValueError(f"Could not find data for {year}q{quarter}")
        
        logger.info(f"Found download URL: {download_url}")
        
        # Create directory
        os.makedirs("./data/financial_statement_data_sets", exist_ok=True)
        
        # Download with delay
        time.sleep(1)
        local_filename = f"./data/financial_statement_data_sets/{target_filename}"
        logger.info(f"Downloading to: {local_filename}")
        
        response = requests.get(download_url, headers=headers)
        logger.info(f"Download status code: {response.status_code}")
        
        if response.status_code != 200:
            raise ValueError(f"Download failed with status code: {response.status_code}")
        
        with open(local_filename, 'wb') as f:
            f.write(response.content)
            
        logger.info(f"Successfully downloaded {target_filename}")
        return [[local_filename, target_filename]]
    
    except Exception as e:
        logger.error(f"Error downloading SEC data: {str(e)}")
        raise
    """Download SEC financial statement data sets"""
    try:
        # Use the SEC's EDGAR URLs
        base_url = "https://www.sec.gov/dera/data/financial-statement-data-sets.html"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        # First get the main page
        response = requests.get(base_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all links in the table
        links = soup.find_all('a')
        target_filename = f"{year}q{quarter}.zip"
        download_url = None
        
        # Find the matching link
        for link in links:
            if target_filename in link.get('href', ''):
                download_url = f"https://www.sec.gov{link['href']}"
                break
        
        if not download_url:
            raise ValueError(f"Could not find data for {year}q{quarter}")
        
        # Create directory if it doesn't exist
        os.makedirs("./data/financial_statement_data_sets", exist_ok=True)
        
        # Download the file with a delay to respect rate limits
        time.sleep(1)  # Add a delay between requests
        local_filename = f"./data/financial_statement_data_sets/{target_filename}"
        response = requests.get(download_url, headers=headers)
        
        if response.status_code != 200:
            raise ValueError(f"Download failed with status code: {response.status_code}")
        
        with open(local_filename, 'wb') as f:
            f.write(response.content)
            
        logger.info(f"Downloaded {target_filename}")
        return [[local_filename, target_filename]]
    
    except Exception as e:
        logger.error(f"Error downloading SEC data: {str(e)}")
        raise 

    """Download SEC financial statement data sets"""
    try:
        # Direct URL pattern for SEC data
        download_url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
        
        # Create directory if it doesn't exist
        os.makedirs("./data/financial_statement_data_sets", exist_ok=True)
        
        # Download the file
        local_filename = f"./data/financial_statement_data_sets/{year}q{quarter}.zip"
        
        # Use headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(download_url, headers=headers)
        
        if response.status_code != 200:
            raise ValueError(f"Could not download data for {year}q{quarter}. Status code: {response.status_code}")
        
        with open(local_filename, 'wb') as f:
            f.write(response.content)
            
        logger.info(f"Downloaded {year}q{quarter}.zip")
        return [[local_filename, f"{year}q{quarter}.zip"]]
    
    except Exception as e:
        logger.error(f"Error downloading SEC data: {str(e)}")
        raise
    """Download SEC financial statement data sets"""
    try:
        base_url = "https://www.sec.gov/dera/data/financial-statement-data-sets.html"
        response = requests.get(base_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all links in the table
        links = soup.find_all('a')
        target_filename = f"{year}q{quarter}.zip"
        download_url = None
        
        # Find the matching link
        for link in links:
            if target_filename in link.get('href', ''):
                download_url = f"https://www.sec.gov{link['href']}"
                break
        
        if not download_url:
            raise ValueError(f"Could not find data for {year}q{quarter}")
        
        # Create directory if it doesn't exist
        os.makedirs("./data/financial_statement_data_sets", exist_ok=True)
        
        # Download the file
        local_filename = f"./data/financial_statement_data_sets/{target_filename}"
        response = requests.get(download_url)
        
        with open(local_filename, 'wb') as f:
            f.write(response.content)
            
        logger.info(f"Downloaded {target_filename}")
        return [[local_filename, target_filename]]
    
    except Exception as e:
        logger.error(f"Error downloading SEC data: {str(e)}")
        raise

def upload_to_s3(downloaded_files, year, quarter):
    """Upload zip and extracted files to S3"""
    try:
        logger.info(f"Starting upload_to_s3 with downloaded_files: {downloaded_files}")
        
        bucket_name = "sec-finance-data-team1"
        temp_dir = "/tmp/sec_data"
        
        # Create temp directory
        os.makedirs(temp_dir, mode=0o777, exist_ok=True)
        logger.info(f"Created temp directory: {temp_dir}")
        
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Handle the downloaded_files parameter
        if isinstance(downloaded_files, str):
            # If it's a string (from XCom), evaluate it
            import ast
            downloaded_files = ast.literal_eval(downloaded_files)
        
        # Get the actual file path
        zip_path = downloaded_files[0][0]  # First item's first element
        logger.info(f"Using zip file path: {zip_path}")
        
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"Zip file not found at: {zip_path}")
            
        # Extract and upload files
        with ZipFile(zip_path) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                # Extract file to temp directory
                zip_file.extract(filename, temp_dir)
                temp_file = os.path.join(temp_dir, filename)
                
                # Upload to S3 using your existing structure
                s3_key = f"sec_data/{year}q{quarter}/raw/{filename}"
                logger.info(f"Uploading {temp_file} to {bucket_name}/{s3_key}")
                
                s3_client.upload_file(temp_file, bucket_name, s3_key)
                logger.info(f"Successfully uploaded {filename}")
                
                # Clean up temp file
                os.remove(temp_file)
        
        return True
        
    except Exception as e:
        logger.error(f"Error in upload_to_s3: {str(e)}")
        raise
    """Upload zip and extracted files to S3"""
    try:
        logger.info(f"Starting upload_to_s3 with downloaded_files: {downloaded_files}")
        
        bucket_name = "sec-finance-data-team1"
        temp_dir = "/tmp/sec_data"
        
        # Create temp directory
        os.makedirs(temp_dir, mode=0o777, exist_ok=True)
        logger.info(f"Created temp directory: {temp_dir}")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1'
        )
        
        # Get the actual file path (first element of the nested list)
        zip_path = downloaded_files[0][0]
        logger.info(f"Processing zip file: {zip_path}")
        
        # Extract and upload files
        with ZipFile(zip_path) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                # Extract file to temp directory
                zip_file.extract(filename, temp_dir)
                temp_file = os.path.join(temp_dir, filename)
                
                # Upload to S3 using your existing structure
                s3_key = f"sec_data/{year}q{quarter}/raw/{filename}"
                s3_client.upload_file(temp_file, bucket_name, s3_key)
                logger.info(f"Uploaded {filename} to S3: {s3_key}")
                
                # Clean up temp file
                os.remove(temp_file)
        
        return True
        
    except Exception as e:
        logger.error(f"Error in upload_to_s3: {str(e)}")
        raise
    """Upload zip and extracted files to S3"""
    try:
        logger.info(f"Starting upload_to_s3 with downloaded_files: {downloaded_files}")
        
        bucket_name = "sec-finance-data-team1"
        temp_dir = "/tmp/sec_data"
        
        # Create temp directory
        os.makedirs(temp_dir, mode=0o777, exist_ok=True)
        logger.info(f"Created temp directory: {temp_dir}")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1'
        )
        
        # Get the actual file path (first element of the first list item)
        zip_path = downloaded_files[0] if isinstance(downloaded_files, list) else downloaded_files
        logger.info(f"Processing zip file: {zip_path}")
        
        # Extract and upload files
        with ZipFile(zip_path) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                # Extract file to temp directory
                zip_file.extract(filename, temp_dir)
                temp_file = os.path.join(temp_dir, filename)
                
                # Upload to S3
                s3_key = f"{year}/Q{quarter}/{filename}"
                s3_client.upload_file(temp_file, bucket_name, s3_key)
                logger.info(f"Uploaded {filename} to S3: {s3_key}")
                
                # Clean up temp file
                os.remove(temp_file)
        
        return True
        
    except Exception as e:
        logger.error(f"Error in upload_to_s3: {str(e)}")
        raise
    """Upload zip and extracted files to S3"""
    try:
        # Debug logging
        logger.info(f"Starting upload_to_s3 with downloaded_files: {downloaded_files}")
        
        bucket_name = "sec-finance-data-team1"
        temp_dir = "/tmp/sec_data"
        
        # Create temp directory with full permissions
        os.makedirs(temp_dir, mode=0o777, exist_ok=True)
        logger.info(f"Created temp directory: {temp_dir}")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1'
        )
        
        # Get the actual file path from downloaded_files
        zip_path = downloaded_files[0][0] if isinstance(downloaded_files, list) else downloaded_files
        logger.info(f"Processing zip file: {zip_path}")
        
        # Extract and upload files
        with ZipFile(zip_path) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                # Extract file to temp directory
                zip_file.extract(filename, temp_dir)
                temp_file = os.path.join(temp_dir, filename)
                
                # Upload to S3
                s3_key = f"{year}/Q{quarter}/{filename}"
                s3_client.upload_file(temp_file, bucket_name, s3_key)
                logger.info(f"Uploaded {filename} to S3: {s3_key}")
                
                # Clean up temp file
                os.remove(temp_file)
        
        return True
        
    except Exception as e:
        logger.error(f"Error in upload_to_s3: {str(e)}")
        raise
    """Upload zip and extracted files to S3"""
    try:
        # Debug logging
        logger.info(f"Starting upload_to_s3 with downloaded_files: {downloaded_files}")
        
        bucket_name = "sec-finance-data-team1"
        temp_dir = "/tmp/sec_data"
        
        # Create temp directory with full permissions
        os.makedirs(temp_dir, mode=0o777, exist_ok=True)
        logger.info(f"Created temp directory: {temp_dir}")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1'
        )
        
        zip_path = downloaded_files[0][0]
        logger.info(f"Processing zip file: {zip_path}")
        
        # Extract and upload files
        with ZipFile(zip_path) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                try:
                    # Extract file to temp directory
                    temp_file = os.path.join(temp_dir, filename)
                    logger.info(f"Extracting {filename} to {temp_file}")
                    
                    with zip_file.open(filename) as source, open(temp_file, 'wb') as target:
                        target.write(source.read())
                    
                    # Read file content
                    with open(temp_file, 'rb') as file:
                        file_content = file.read()
                    
                    # Upload to S3
                    raw_key = f"sec_data/{year}q{quarter}/raw/{filename}"
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=raw_key,
                        Body=file_content
                    )
                    
                    logger.info(f"Successfully uploaded {filename} to S3: {raw_key}")
                    
                    # Clean up temp file
                    os.remove(temp_file)
                    logger.info(f"Cleaned up temp file: {temp_file}")
                    
                except Exception as e:
                    logger.error(f"Error uploading {filename}: {str(e)}")
                    raise

        # Clean up temp directory and any remaining files
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.info("Cleaned up temp directory")

        # Clean up downloaded files
        shutil.rmtree("./data/financial_statement_data_sets", ignore_errors=True)
        logger.info("Cleaned up temporary directory: ./data/financial_statement_data_sets")
        
        return True

    except Exception as e:
        logger.error(f"Error in upload_to_s3: {str(e)}")
        raise
def process_and_load_to_snowflake(ds=None, **kwargs):
    """Process and load data to Snowflake"""
    try:
        # Get AWS connection
        aws_conn = BaseHook.get_connection('aws_default')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name='us-east-1'
        )

        # Create temp directory
        temp_dir = "/tmp"
        
        # Get Snowflake hook
        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Create stage if it doesn't exist
        snow_hook.run("CREATE STAGE IF NOT EXISTS sec_stage")
        
        # Process each file
        files = {
            'raw_num': 'num.txt',
            'raw_pre': 'pre.txt',
            'raw_sub': 'sub.txt',
            'raw_tag': 'tag.txt'
        }
        
        for table_name, filename in files.items():
            try:
                # Download from S3
                bucket_name = "sec-finance-data-team1"
                s3_key = f"sec_data/2023q4/raw/{filename}"
                temp_file = os.path.join(temp_dir, filename)
                
                logger.info(f"Downloading {s3_key} from S3...")
                s3_client.download_file(bucket_name, s3_key, temp_file)
                
                # Read number of records
                with open(temp_file, 'r') as f:
                    num_records = sum(1 for _ in f)
                logger.info(f"Read {filename} from S3: {num_records} records")
                
                # Create tables if they don't exist
                if table_name == 'raw_num':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_num (
                        adsh VARCHAR,
                        tag VARCHAR,
                        version VARCHAR,
                        coreg VARCHAR,
                        ddate VARCHAR,
                        qtrs VARCHAR,
                        uom VARCHAR,
                        value VARCHAR,
                        footnote VARCHAR
                    )
                    """)
                elif table_name == 'raw_pre':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_pre (
                        adsh VARCHAR,
                        report VARCHAR,
                        line VARCHAR,
                        stmt VARCHAR,
                        inpth VARCHAR,
                        rfile VARCHAR,
                        tag VARCHAR,
                        version VARCHAR,
                        plabel VARCHAR,
                        negating VARCHAR
                    )
                    """)
                elif table_name == 'raw_sub':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_sub (
                        adsh VARCHAR,
                        cik VARCHAR,
                        name VARCHAR,
                        sic VARCHAR,
                        countryba VARCHAR,
                        stprba VARCHAR,
                        cityba VARCHAR,
                        zipba VARCHAR,
                        bas1 VARCHAR,
                        bas2 VARCHAR,
                        baph VARCHAR,
                        countryma VARCHAR,
                        stprma VARCHAR,
                        cityma VARCHAR,
                        zipma VARCHAR,
                        mas1 VARCHAR,
                        mas2 VARCHAR,
                        countryinc VARCHAR,
                        stprinc VARCHAR,
                        ein VARCHAR,
                        former VARCHAR,
                        changed VARCHAR,
                        afs VARCHAR,
                        wksi VARCHAR,
                        fye VARCHAR,
                        form VARCHAR,
                        period VARCHAR,
                        fy VARCHAR,
                        fp VARCHAR,
                        filed VARCHAR,
                        accepted VARCHAR,
                        prevrpt VARCHAR,
                        detail VARCHAR,
                        instance VARCHAR,
                        nciks VARCHAR,
                        aciks VARCHAR,
                        pubfloatusd VARCHAR,
                        floatdate VARCHAR,
                        floataxis VARCHAR,
                        floatmems VARCHAR
                    )
                    """)
                elif table_name == 'raw_tag':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_tag (
                        tag VARCHAR,
                        version VARCHAR,
                        custom VARCHAR,
                        abstract VARCHAR,
                        datatype VARCHAR,
                        iord VARCHAR,
                        crdr VARCHAR,
                        tlabel VARCHAR,
                        doc VARCHAR
                    )
                    """)
                
                logger.info(f"Putting {filename} to Snowflake stage...")
                # Put file to stage
                snow_hook.run(f"PUT file://{temp_file} @sec_stage/{filename}")
                
                logger.info(f"Copying {filename} into {table_name}...")
                # Copy into table with corrected file format options
                copy_query = f"""
                COPY INTO {table_name}
                FROM @sec_stage/{filename}
                FILE_FORMAT = (
                    TYPE = CSV 
                    FIELD_DELIMITER = '\t'
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    ESCAPE_UNENCLOSED_FIELD = NONE
                    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                    NULL_IF = ('\\N', '')
                    TRIM_SPACE = TRUE
                )
                ON_ERROR = 'CONTINUE'
                """
                snow_hook.run(copy_query)
                
                logger.info(f"Removing staged file {filename}...")
                # Remove staged file
                snow_hook.run(f"REMOVE @sec_stage/{filename}")
                
                # Clean up temp file
                os.remove(temp_file)
                logger.info(f"Successfully processed {filename}")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                # Clean up temp file if it exists
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                raise
        
        logger.info("Successfully processed all files")
        return True
        
    except Exception as e:
        logger.error(f"Error in process_and_load_to_snowflake: {str(e)}")
        raise
    """Process and load data to Snowflake"""
    try:
        # Get AWS connection
        aws_conn = BaseHook.get_connection('aws_default')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name='us-east-1'
        )

        # Create temp directory
        temp_dir = "/tmp"
        
        # Get Snowflake hook
        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Create stage if it doesn't exist
        snow_hook.run("CREATE STAGE IF NOT EXISTS sec_stage")
        
        # Process each file
        files = {
            'raw_num': 'num.txt',
            'raw_pre': 'pre.txt',
            'raw_sub': 'sub.txt',
            'raw_tag': 'tag.txt'
        }
        
        for table_name, filename in files.items():
            try:
                # Download from S3
                bucket_name = "sec-finance-data-team1"
                s3_key = f"sec_data/2023q4/raw/{filename}"
                temp_file = os.path.join(temp_dir, filename)
                
                logger.info(f"Downloading {s3_key} from S3...")
                s3_client.download_file(bucket_name, s3_key, temp_file)
                
                # Read number of records
                with open(temp_file, 'r') as f:
                    num_records = sum(1 for _ in f)
                logger.info(f"Read {filename} from S3: {num_records} records")
                
                # Create tables if they don't exist
                if table_name == 'raw_num':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_num (
                        adsh VARCHAR,
                        tag VARCHAR,
                        version VARCHAR,
                        coreg VARCHAR,
                        ddate VARCHAR,
                        qtrs VARCHAR,
                        uom VARCHAR,
                        value VARCHAR,
                        footnote VARCHAR
                    )
                    """)
                # ... (other table creation statements remain the same)
                
                logger.info(f"Putting {filename} to Snowflake stage...")
                # Put file to stage
                snow_hook.run(f"PUT file://{temp_file} @sec_stage/{filename}")
                
                logger.info(f"Copying {filename} into {table_name}...")
                # Copy into table with enhanced file format options
                copy_query = f"""
                COPY INTO {table_name}
                FROM @sec_stage/{filename}
                FILE_FORMAT = (
                    TYPE = CSV 
                    FIELD_DELIMITER = '\t'
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    ESCAPE_UNENCLOSED_FIELD = NONE
                    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                    VALIDATE_UTF8 = FALSE
                    EMPTY_FIELD_AS_NULL = TRUE
                    REPLACE_INVALID_CHARACTERS = TRUE
                )
                ON_ERROR = 'CONTINUE'
                """
                snow_hook.run(copy_query)
                
                logger.info(f"Removing staged file {filename}...")
                # Remove staged file
                snow_hook.run(f"REMOVE @sec_stage/{filename}")
                
                # Clean up temp file
                os.remove(temp_file)
                logger.info(f"Successfully processed {filename}")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                # Clean up temp file if it exists
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                raise
        
        logger.info("Successfully processed all files")
        return True
        
    except Exception as e:
        logger.error(f"Error in process_and_load_to_snowflake: {str(e)}")
        raise
    """Process and load data to Snowflake"""
    try:
        # Get AWS connection
        aws_conn = BaseHook.get_connection('aws_default')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name='us-east-1'
        )

        # Create temp directory
        temp_dir = "/tmp"
        
        # Get Snowflake hook
        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Create stage if it doesn't exist
        snow_hook.run("CREATE STAGE IF NOT EXISTS sec_stage")
        
        # Process each file
        files = {
            'raw_num': 'num.txt',
            'raw_pre': 'pre.txt',
            'raw_sub': 'sub.txt',
            'raw_tag': 'tag.txt'
        }
        
        for table_name, filename in files.items():
            try:
                # Download from S3
                bucket_name = "sec-finance-data-team1"
                s3_key = f"sec_data/2023q4/raw/{filename}"
                temp_file = os.path.join(temp_dir, filename)
                
                s3_client.download_file(bucket_name, s3_key, temp_file)
                
                # Read number of records
                with open(temp_file, 'r') as f:
                    num_records = sum(1 for _ in f)
                logger.info(f"Read {filename} from S3: {num_records} records")
                
                # Create tables if they don't exist
                if table_name == 'raw_num':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_num (
                        adsh VARCHAR,
                        tag VARCHAR,
                        version VARCHAR,
                        coreg VARCHAR,
                        ddate VARCHAR,
                        qtrs VARCHAR,
                        uom VARCHAR,
                        value VARCHAR,
                        footnote VARCHAR
                    )
                    """)
                elif table_name == 'raw_pre':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_pre (
                        adsh VARCHAR,
                        report VARCHAR,
                        line VARCHAR,
                        stmt VARCHAR,
                        inpth VARCHAR,
                        rfile VARCHAR,
                        tag VARCHAR,
                        version VARCHAR,
                        plabel VARCHAR,
                        negating VARCHAR
                    )
                    """)
                elif table_name == 'raw_sub':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_sub (
                        adsh VARCHAR,
                        cik VARCHAR,
                        name VARCHAR,
                        sic VARCHAR,
                        countryba VARCHAR,
                        stprba VARCHAR,
                        cityba VARCHAR,
                        zipba VARCHAR,
                        bas1 VARCHAR,
                        bas2 VARCHAR,
                        baph VARCHAR,
                        countryma VARCHAR,
                        stprma VARCHAR,
                        cityma VARCHAR,
                        zipma VARCHAR,
                        mas1 VARCHAR,
                        mas2 VARCHAR,
                        countryinc VARCHAR,
                        stprinc VARCHAR,
                        ein VARCHAR,
                        former VARCHAR,
                        changed VARCHAR,
                        afs VARCHAR,
                        wksi VARCHAR,
                        fye VARCHAR,
                        form VARCHAR,
                        period VARCHAR,
                        fy VARCHAR,
                        fp VARCHAR,
                        filed VARCHAR,
                        accepted VARCHAR,
                        prevrpt VARCHAR,
                        detail VARCHAR,
                        instance VARCHAR,
                        nciks VARCHAR,
                        aciks VARCHAR,
                        pubfloatusd VARCHAR,
                        floatdate VARCHAR,
                        floataxis VARCHAR,
                        floatmems VARCHAR
                    )
                    """)
                elif table_name == 'raw_tag':
                    snow_hook.run("""
                    CREATE TABLE IF NOT EXISTS raw_tag (
                        tag VARCHAR,
                        version VARCHAR,
                        custom VARCHAR,
                        abstract VARCHAR,
                        datatype VARCHAR,
                        iord VARCHAR,
                        crdr VARCHAR,
                        tlabel VARCHAR,
                        doc VARCHAR
                    )
                    """)
                
                # Put file to stage
                snow_hook.run(f"PUT file://{temp_file} @sec_stage/{filename}")
                
                # Copy into table
                snow_hook.run(f"""
                COPY INTO {table_name}
                FROM @sec_stage/{filename}
                FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\t' SKIP_HEADER = 1)
                """)
                
                # Remove staged file
                snow_hook.run(f"REMOVE @sec_stage/{filename}")
                
                # Clean up temp file
                os.remove(temp_file)
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                raise
        
        return True
        
    except Exception as e:
        logger.error(f"Error in process_and_load_to_snowflake: {str(e)}")
        raise

