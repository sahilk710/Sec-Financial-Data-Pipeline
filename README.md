Assignment 2 Team 1
Project Overview
This project involves designing a financial statement database for Findata Inc., a fintech company supporting analysts conducting fundamental analysis of US public companies. The database will be implemented using Snowflake and sourced from SEC Financial Statement Data Sets.
Team Contribution Declaration
WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK.
Contribution:
Dhrumil: 33 %
Sahil: 33 %
Husain: 33 %
GitHub Repository
Repository Link: https://github.com/sahilk710/Sec-Financial-Data-Pipeline
Frontend : https://sec-financial-data-pipeline-nb6pwqewrtxm6irwf8qrxk.streamlit.app/  
Backend : https://finance-data-pipeline.uk.r.appspot.com/docs#/default/debug_secrets_debug_secrets_get


 


Problem Statement
Financial analysts rely on accurate, structured, and timely financial data for fundamental analysis of public companies. However, SEC financial statements are provided in raw, unstructured formats, making it difficult to standardize and store efficiently for downstream analysis. This project aims to build a scalable financial statement database using Snowflake, integrating data pipelines for extraction, transformation, validation, and storage.
Challenges Addressed:
Efficiently extracting financial statement datasets from the SEC website.
Transforming and structuring raw financial data for analysis.
Ensuring data integrity through validation techniques before storage.
Automating the ETL process for continuous ingestion and transformation.
Desired Outcome:A fully functional data pipeline that scrapes SEC datasets, processes financial data into structured formats, and stores it in Snowflake for querying and analysis. The system will also validate the data and support an interactive interface for financial exploration.
Constraints & Requirements:
Use BeautifulSoup for SEC data scraping.
Store extracted data in AWS S3 (both raw and JSON formats).
Process and transform data into denormalized fact tables in Snowflake.
Implement DBT (Data Build Tool) for transformation and validation.
Automate the workflow using Apache Airflow.
Develop a Streamlit app for querying and visualization.
Proof of Concept
 Technologies 
BeautifulSoup: Efficient and widely used web scraping library for extracting SEC financial datasets.
AWS S3: Serves as an intermediate data lake for raw and JSON-formatted data before transformation.
Snowflake: Cloud-based data warehousing solution for structured financial data storage and fast querying.
DBT (Data Build Tool): Applied for data transformation and validation before loading into denormalized fact tables.
Apache Airflow: Orchestrates the ETL pipelines for automated data ingestion and processing.
FastAPI: Provides a RESTful API to enable programmatic access to financial data.
Streamlit: Frontend for interactive querying and visualization.
Initial Setup & Tests:
Implemented SEC data scraping using BeautifulSoup to retrieve dataset links.
Successfully stored extracted data into AWS S3 in both raw and JSON formats.
Created Snowflake schemas for raw staging, JSON storage, and denormalized fact tables.
Developed DBT models for transforming raw SEC data into structured financial tables.
Tested Apache Airflow pipelines for automated extraction, transformation, and loading (ETL) into Snowflake.







Workflows:-

User Interaction Flow:
User >> Streamlit App >> FastAPI App >> Snowflake Database


Data Extraction Flow:
      Airflow DAG >>SEC Website >> BeautifulSoup Scraper >> AWS S3 (Raw & JSON)
Processing & Storage:

Airflow DAG> >S3 >>RAW >> Snowflake 
Airflow DAG> >S3 >>JSON >> Snowflake 
Airflow DAG>>Raw Snowflake>> DBT>>Fact tables Snowflake 
Deployment & Access:
GitHub >> Public Cloud Platform (GCP/AWS)










Project Structure

sec-financial-data/
├── airflow/
│   ├── dags/
│   │   ├── scraping_pipeline.py       # Scrapes SEC data
│   │   ├── s3_storage_pipeline.py     # Stores scraped data in AWS S3 (raw & JSON)
│   │   ├── snowflake_ingestion.py     # Moves raw & JSON data from S3 to Snowflake
│   │   ├── transformation_pipeline.py # Applies DBT transformations on Snowflake data
│   │   ├── validation_pipeline.py     # Runs data validation checks
│   │   └── fact_tables_pipeline.py    # Stores validated, transformed data into fact tables
│   └── config/
│       └── job_definition.yaml
├── validation/
│   ├── validators/
│   │   ├── schema_validator.py
│   │   ├── data_validator.py
│   │   └── relationship_validator.py
│   └── validation_strategy.md
├── snowflake/
│   ├── schemas/
│   │   ├── raw_staging.sql
│   │   ├── json_schema.sql
│   │   └── fact_tables.sql
│   └── scripts/
│       ├── load_raw.sql
│       ├── transform_json.sql
│       └── create_fact_tables.sql
├── api/
│   ├── main.py
│   ├── database.py
│   └── models.py
├── streamlit/
│   └── app.py
├── tests/
│   ├── test_validation.py
│   ├── test_transformations.py
│   └── test_api.py
└── docs/
    ├── README.md
    ├── AiUseDisclosure.md
    └── testing_strategy.md
Walkthrough of the Application

Step-by-Step Instructions:
Users access the Streamlit App through a web interface.
They can input a year and quarter to retrieve financial datasets from the SEC and also which type of data they want from Raw / Fact tables.
The app provides interactive visualizations and query options.
The Streamlit app triggers API calls to FastAPI.
FastAPI processes user requests and Fetches raw or processed data from Snowflake  based on user input.
Workflow using airflow 
 





![WhatsApp Image 2025-03-15 at 4 32 21 PM](https://github.com/user-attachments/assets/779e99e5-0fc6-4308-8214-93e7c4970bd7)
