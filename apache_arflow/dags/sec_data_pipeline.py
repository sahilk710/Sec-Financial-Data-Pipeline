import requests
from bs4 import BeautifulSoup
import os
import boto3
from botocore.exceptions import ClientError
import time
import zipfile
import json
import pandas as pd
from zipfile import ZipFile
import logging
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_and_download(year, quarter):
    """
    Download and extract SEC financial statement data for a specific year and quarter
    """
    logger.info(f"Starting scrape_and_download function for {year} Q{quarter}")
    url = "https://www.sec.gov/dera/data/financial-statement-data-sets"
    
    # Create local directories
    base_dir = "./data"
    local_dir = f"{base_dir}/financial_statement_data_sets"
    temp_dir = f"{local_dir}/temp"
    os.makedirs(temp_dir, exist_ok=True)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Your Name your.email@example.com)',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Host': 'www.sec.gov'
    }
    
    try:
        logger.info(f"Requesting URL: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for links containing the year and quarter
        target_file = f"{year}q{quarter}"
        
        # Find all links on the page
        links = soup.find_all('a', href=True)
        
        for link in links:
            if target_file in link['href'].lower():
                file_url = f"https://www.sec.gov{link['href']}"
                file_name = f"{target_file}.zip"
                local_file_path = os.path.join(local_dir, file_name)
                
                logger.info(f"Downloading: {file_url}")
                time.sleep(0.1)
                
                with requests.get(file_url, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    with open(local_file_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                
                logger.info(f"Downloaded zip file to: {local_file_path}")
                return [(local_file_path, file_name)]
        
        raise Exception(f"No files found for {year} Q{quarter}")
        
    except Exception as e:
        logger.error(f"Error in scrape_and_download: {str(e)}")
        raise

def process_sec_files(downloaded_files):
    """
    Process SEC data files and convert to JSON format
    """
    try:
        processed_data = {}
        
        # Get the zip file path from downloaded_files
        zip_path = downloaded_files[0][0] if downloaded_files else None
        
        if not zip_path or not os.path.exists(zip_path):
            raise Exception(f"Zip file not found at path: {zip_path}")

        logger.info(f"Processing zip file: {zip_path}")
        
        # Extract and process files
        with ZipFile(zip_path) as myzip:
            # Process num.txt
            with myzip.open('num.txt') as myfile:
                df_num = pd.read_table(myfile, delimiter="\t", low_memory=False)
                processed_data['num'] = df_num.to_dict(orient='records')
                logger.info(f"Processed num.txt: {len(df_num)} records")

            # Process pre.txt
            with myzip.open('pre.txt') as myfile:
                df_pre = pd.read_table(myfile, delimiter="\t")
                processed_data['pre'] = df_pre.to_dict(orient='records')
                logger.info(f"Processed pre.txt: {len(df_pre)} records")

            # Process sub.txt
            with myzip.open('sub.txt') as myfile:
                df_sub = pd.read_table(myfile, delimiter="\t", low_memory=False)
                processed_data['sub'] = df_sub.to_dict(orient='records')
                logger.info(f"Processed sub.txt: {len(df_sub)} records")

            # Process tag.txt
            with myzip.open('tag.txt') as myfile:
                df_tag = pd.read_table(myfile, delimiter="\t")
                processed_data['tag'] = df_tag.to_dict(orient='records')
                logger.info(f"Processed tag.txt: {len(df_tag)} records")

        return processed_data

    except Exception as e:
        logger.error(f"Error processing SEC files: {str(e)}")
        raise


def upload_to_s3(processed_data, downloaded_files, year, quarter):
    """
    Upload both JSON and raw text files to S3
    """
    try:
        bucket_name = "sec-finance-data-team1"
        
        # Configure AWS credentials with SSL verification disabled
        session = boto3.Session(    

    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
    )
        # Create S3 client with SSL verification disabled and transfer configuration
        s3 = session.client(
            's3',
            verify=False,
            config=boto3.session.Config(
                signature_version='s3v4',
                retries={'max_attempts': 3},
                max_pool_connections=25
            )
        )

        # Configure transfer settings for large files
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=8 * 1024 * 1024,  # 8MB
            max_concurrency=10,
            multipart_chunksize=8 * 1024 * 1024,  # 8MB
            use_threads=True
        )

        # Upload raw text files first
        with ZipFile(downloaded_files[0][0]) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                try:
                    # Save raw file to temp location first
                    temp_raw_file = f"/tmp/{filename}"
                    with zip_file.open(filename) as file, open(temp_raw_file, 'wb') as temp_file:
                        temp_file.write(file.read())
                    
                    # Upload raw text file using transfer manager
                    raw_key = f"sec_data/{year}q{quarter}/raw/{filename}"
                    transfer = boto3.s3.transfer.S3Transfer(client=s3, config=transfer_config)
                    transfer.upload_file(
                        temp_raw_file,
                        bucket_name,
                        raw_key
                    )
                    
                    # Clean up temp file
                    os.remove(temp_raw_file)
                    logger.info(f"Successfully uploaded raw {filename} to S3: {raw_key}")
                except Exception as e:
                    logger.error(f"Error uploading raw {filename} to S3: {str(e)}")
                    if os.path.exists(temp_raw_file):
                        os.remove(temp_raw_file)

        # Upload processed JSON files
        for file_type, data in processed_data.items():
            try:
                # Create the S3 key with proper directory structure
                json_key = f"sec_data/{year}q{quarter}/json/{file_type}.json"
                
                # Save JSON to temporary file
                temp_file = f"/tmp/{file_type}.json"
                with open(temp_file, 'w') as f:
                    json.dump(data, f)
                
                # Upload JSON file using transfer manager
                transfer = boto3.s3.transfer.S3Transfer(client=s3, config=transfer_config)
                transfer.upload_file(
                    temp_file,
                    bucket_name,
                    json_key
                )
                
                logger.info(f"Successfully uploaded {file_type}.json to S3: {json_key}")
                
            except Exception as e:
                logger.error(f"Error uploading {file_type}.json to S3: {str(e)}")
            finally:
                # Clean up temp file
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    except Exception as e:
        logger.error(f"Error in upload_to_s3: {str(e)}")
        raise
    """
    Upload both JSON and raw text files to S3
    """
    try:
        bucket_name = "sec-finance-data-team1"
        
        # Configure AWS credentials with SSL verification disabled
        session = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)
        
        # Create S3 client with SSL verification disabled
        s3 = session.client(
            's3',
            verify=False,  # Disable SSL verification
            config=boto3.session.Config(
                signature_version='s3v4',
                retries={'max_attempts': 3}
            )
        )

        # Upload raw text files first
        with ZipFile(downloaded_files[0][0]) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                try:
                    with zip_file.open(filename) as file:
                        raw_data = file.read()
                        
                    # Upload raw text file
                    raw_key = f"sec_data/{year}q{quarter}/raw/{filename}"
                    s3.put_object(
                        Bucket=bucket_name,
                        Key=raw_key,
                        Body=raw_data
                    )
                    logger.info(f"Successfully uploaded raw {filename} to S3: {raw_key}")
                except Exception as e:
                    logger.error(f"Error uploading raw {filename} to S3: {str(e)}")

        # Upload processed JSON files
        for file_type, data in processed_data.items():
            try:
                # Create the S3 key with proper directory structure
                json_key = f"sec_data/{year}q{quarter}/json/{file_type}.json"
                
                # Save JSON to temporary file
                temp_file = f"/tmp/{file_type}.json"
                with open(temp_file, 'w') as f:
                    json.dump(data, f)
                
                # Upload JSON file
                s3.upload_file(
                    temp_file,
                    bucket_name,
                    json_key
                )
                
                logger.info(f"Successfully uploaded {file_type}.json to S3: {json_key}")
                
            except Exception as e:
                logger.error(f"Error uploading {file_type}.json to S3: {str(e)}")
            finally:
                # Clean up temp file
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    except Exception as e:
        logger.error(f"Error in upload_to_s3: {str(e)}")
        raise
    """
    Upload both JSON and raw text files to S3
    """
    try:
        bucket_name = "sec-finance-data-team1"
        
        # Configure AWS credentials
        session = boto3.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
        )
        s3 = session.client('s3')

        # Upload raw text files first
        with ZipFile(downloaded_files[0][0]) as zip_file:
            for filename in ['num.txt', 'pre.txt', 'sub.txt', 'tag.txt']:
                try:
                    with zip_file.open(filename) as file:
                        raw_data = file.read()
                        
                    # Upload raw text file
                    raw_key = f"sec_data/{year}q{quarter}/raw/{filename}"
                    s3.put_object(
                        Bucket=bucket_name,
                        Key=raw_key,
                        Body=raw_data
                    )
                    logger.info(f"Successfully uploaded raw {filename} to S3: {raw_key}")
                except Exception as e:
                    logger.error(f"Error uploading raw {filename} to S3: {str(e)}")

        # Upload processed JSON files
        for file_type, data in processed_data.items():
            try:
                # Create the S3 key with proper directory structure
                json_key = f"sec_data/{year}q{quarter}/json/{file_type}.json"
                
                # Save JSON to temporary file
                temp_file = f"/tmp/{file_type}.json"
                with open(temp_file, 'w') as f:
                    json.dump(data, f)
                
                # Upload JSON file
                s3.upload_file(
                    temp_file,
                    bucket_name,
                    json_key
                )
                
                logger.info(f"Successfully uploaded {file_type}.json to S3: {json_key}")
                
            except Exception as e:
                logger.error(f"Error uploading {file_type}.json to S3: {str(e)}")
            finally:
                # Clean up temp file
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    except Exception as e:
        logger.error(f"Error in upload_to_s3: {str(e)}")
        raise

def cleanup_files():
    """
    Clean up temporary files
    """
    try:
        temp_dir = "./data/financial_statement_data_sets"
        if os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up temporary directory: {temp_dir}")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

def main():
    """
    Main function to run the pipeline
    """
    try:
        # Set the year and quarter
        year = "2024"
        quarter = "4"
        
        # Download and extract files
        downloaded_files = scrape_and_download(year, quarter)
        
        # Process files and convert to JSON
        processed_data = process_sec_files(downloaded_files)
        
        # Upload both raw and processed data to S3
        upload_to_s3(processed_data, downloaded_files, year, quarter)
        
        # Cleanup temporary files
        cleanup_files()
        
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

