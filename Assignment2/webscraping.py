import requests
from bs4 import BeautifulSoup
import re
import os
from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError
import time

# Set AWS credentials (replace with your actual credentials)
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
 
def download_file(url, local_filename):
    with requests.get(url, stream=True, headers=headers) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        block_size = 8192
        with open(local_filename, 'wb') as f, tqdm(
            desc=local_filename,
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar:
            for data in r.iter_content(block_size):
                size = f.write(data)
                progress_bar.update(size)

def upload_to_s3(file_path, bucket_name, s3_key):
    s3_client = boto3.client('s3')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name='us-east-1'  # or your preferred region
    )
    try:
        # Verify the file exists locally
        if not os.path.exists(file_path):
            print(f"Error: Local file {file_path} does not exist")
            return False
            
        # Verify file size
        file_size = os.path.getsize(file_path)
        print(f"Local file size: {file_size} bytes")
        
        # Verify AWS credentials
        try:
            s3_client.list_buckets()
        except Exception as e:
            print(f"AWS credentials error: {e}")
            return False
            
        print(f"Uploading {file_path} to S3 bucket {bucket_name} with key {s3_key}...")
        s3_client.upload_file(file_path, bucket_name, s3_key)
        
        # Verify upload
        try:
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            print(f"Successfully verified upload of {s3_key} to S3 bucket {bucket_name}")
            return True
        except ClientError as e:
            print(f"Failed to verify upload: {e}")
            return False
            
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        error_code = e.response['Error']['Code']
        print(f"Error code: {error_code}")
        return False
    except Exception as e:
        print(f"Unexpected error during S3 upload: {e}")
        return False

def main():
    url = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
    local_dir = "financial_statement_data_sets"
    bucket_name = "sec-finance-data-team1"
    
    # Create the local directory if it doesn't exist
    os.makedirs(local_dir, exist_ok=True)
    
    # Add required headers for SEC.gov
    global headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 (your-email@domain.com)',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Host': 'www.sec.gov'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find the table containing the data
        table = soup.find('table')
        if not table:
            print("Error: Could not find the data table on the page")
            return
            
        # Find all rows in the table body
        rows = table.find_all('tr')
        for row in rows:
            # Find the link in the first column
            link = row.find('a')
            if link and link.get('href'):
                file_url = f"https://www.sec.gov{link['href']}"
                # Extract the quarter/year from the link text (e.g., "2024 Q4")
                file_name = f"{link.text.strip()}.zip"
                local_file_path = os.path.join(local_dir, file_name)
                
                try:
                    print(f"\nProcessing {file_name}...")
                    print(f"Downloading from URL: {file_url}")
                    download_file(file_url, local_file_path)
                    print(f"Downloaded {file_name} to {local_file_path}")
                    
                    # Upload to S3
                    s3_key = f"sec_data/{file_name}"
                    success = upload_to_s3(local_file_path, bucket_name, s3_key)
                    if not success:
                        print(f"Failed to upload {file_name} to S3")
                    
                    # Add a small delay between requests
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"Error processing {file_name}: {e}")
                    continue
                    
    except requests.RequestException as e:
        print(f"Error fetching the SEC webpage: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()