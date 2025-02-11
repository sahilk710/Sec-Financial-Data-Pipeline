import pandas as pd
import snowflake.connector
import boto3
from io import StringIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS S3 Configuration
S3_BUCKET = 'sec-finance-data-team1'
S3_PREFIX = 'sec_data/2024q4/raw/'  # Base path for the txt files

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    'account': 'tr28614.us-east4.gcp',
    'user': 'dev_dhrumil_user',
    'password': 'Dhrumil@123',
    'warehouse': 'COMPUTE_WH',
    'database': 'assignment2_team1',
    'schema': 'raw_staging'
}

def read_s3_files():
    """Read txt files directly from S3"""
    try:
        s3_client = boto3.client('s3',region_name='us-east-1')
        dataframes = {}
        
        # Files to process
        files = ["num.txt", "tag.txt", "pre.txt", "sub.txt"]
        
        for file in files:
            file_path = f"{S3_PREFIX}{file}"
            logger.info(f"Reading {file_path} from S3")
            
            try:
                # Get the file content from S3
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_path)
                content = response['Body'].read().decode('utf-8')
                
                # Read the content into a DataFrame
                df = pd.read_csv(StringIO(content), sep='\t', encoding='utf-8')
                dataframes[file.split('.')[0]] = df
                
                logger.info(f"Successfully loaded {file}")
                
            except Exception as e:
                logger.error(f"Error loading {file}: {str(e)}")
                raise
                
        return dataframes
        
    except Exception as e:
        logger.error(f"Error in reading S3 files: {str(e)}")
        raise


def load_to_snowflake(dataframes):
    """Load the processed data into Snowflake"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        
        # Create table
        create_table_query = """
        CREATE OR REPLACE TABLE balance_sheet (
            adsh STRING,
            cik STRING,
            fiscal_year INT,
            fiscal_period STRING,
            filing_date DATE,
            numeric_value FLOAT
        );
        """
        cursor.execute(create_table_query)
        logger.info("Table created successfully")

        # Merge DataFrames
        merged_df = pd.merge(
            dataframes["num"],
            dataframes["sub"][["adsh", "cik", "fy", "fp", "filed"]],
            on="adsh",
            how="left"
        )

        # Handle missing values and convert date format
        merged_df["fy"] = merged_df["fy"].fillna(0).astype(int)
        merged_df["fp"] = merged_df["fp"].fillna("Unknown")
        merged_df["filed"] = pd.to_datetime(merged_df["filed"].fillna(19000101), format='%Y%m%d')
        merged_df["value"] = merged_df["value"].fillna(0.0)

        # Insert data in batches
        batch_size = 1000
        total_rows = len(merged_df)
        
        insert_query = """
        INSERT INTO balance_sheet
        (adsh, cik, fiscal_year, fiscal_period, filing_date, numeric_value)
        VALUES (%s, %s, %s, %s, TO_DATE(%s), %s)
        """

        for i in range(0, total_rows, batch_size):
            batch = merged_df.iloc[i:i + batch_size]
            for _, row in batch.iterrows():
                try:
                    cursor.execute(insert_query, (
                        row["adsh"],
                        row["cik"],
                        int(row["fy"]),
                        row["fp"],
                        row["filed"].strftime('%Y-%m-%d'),  # Convert to YYYY-MM-DD format
                        float(row["value"])
                    ))
                except Exception as e:
                    logger.error(f"Error inserting row: {str(e)}")
            
            conn.commit()
            logger.info(f"Inserted batch: {i} to {i + len(batch)} rows")

        logger.info("Data successfully loaded into Snowflake")

    except Exception as e:
        logger.error(f"Error in Snowflake loading: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


    """Load the processed data into Snowflake"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        
        # Create table
        create_table_query = """
        CREATE OR REPLACE TABLE balance_sheet (
            adsh STRING,
            cik STRING,
            fiscal_year INT,
            fiscal_period STRING,
            filing_date DATE,
            numeric_value FLOAT
        );
        """
        cursor.execute(create_table_query)
        logger.info("Table created successfully")

        # Merge DataFrames
        merged_df = pd.merge(
            dataframes["num"],
            dataframes["sub"][["adsh", "cik", "fy", "fp", "filed"]],
            on="adsh",
            how="left"
        )

        # Handle missing values
        merged_df["fy"] = merged_df["fy"].fillna(0).astype(int)
        merged_df["fp"] = merged_df["fp"].fillna("Unknown")
        merged_df["filed"] = merged_df["filed"].fillna("1900-01-01")
        merged_df["value"] = merged_df["value"].fillna(0.0)

        # Insert data in batches
        batch_size = 1000
        total_rows = len(merged_df)
        
        insert_query = """
        INSERT INTO balance_sheet
        (adsh, cik, fiscal_year, fiscal_period, filing_date, numeric_value)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        for i in range(0, total_rows, batch_size):
            batch = merged_df.iloc[i:i + batch_size]
            for _, row in batch.iterrows():
                try:
                    cursor.execute(insert_query, (
                        row["adsh"],
                        row["cik"],
                        int(row["fy"]),
                        row["fp"],
                        row["filed"],
                        float(row["value"])
                    ))
                except Exception as e:
                    logger.error(f"Error inserting row: {str(e)}")
            
            conn.commit()
            logger.info(f"Inserted batch: {i} to {i + len(batch)} rows")

        logger.info("Data successfully loaded into Snowflake")

    except Exception as e:
        logger.error(f"Error in Snowflake loading: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    """Main function to orchestrate the data pipeline"""
    try:
        # Read files directly from S3
        dataframes = read_s3_files()
        
        # Load data into Snowflake
        load_to_snowflake(dataframes)
        
        logger.info("Data pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

