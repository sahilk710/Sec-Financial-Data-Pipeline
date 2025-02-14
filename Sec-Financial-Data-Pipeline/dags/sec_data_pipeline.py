from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from test_connections import test_connections
from raw import (download_sec_data, 
                upload_to_s3, 
                process_and_load_to_snowflake)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Define constants
DBT_PROJECT_DIR = "/opt/airflow/dags/financial_dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dags/financial_dbt_project"

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sec_data_pipeline',
    default_args=default_args,
    description='Pipeline to process SEC financial data',
    schedule_interval=None,
    catchup=False
)

# Define tasks
test_conn_task = PythonOperator(
    task_id='test_connections',
    python_callable=test_connections,
    dag=dag
)

download_task = PythonOperator(
    task_id='download_sec_data',
    python_callable=download_sec_data,
    op_kwargs={'year': '2023', 'quarter': '4'},
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={
        'downloaded_files': "{{ ti.xcom_pull(task_ids='download_sec_data') }}", 
        'year': '2023', 
        'quarter': '1'
    },
    dag=dag  # Added missing dag parameter
)

load_task = PythonOperator(
    task_id='process_and_load_to_snowflake',
    python_callable=process_and_load_to_snowflake,
    dag=dag
)

trigger_dbt = TriggerDagRunOperator(
    task_id='trigger_dbt_pipeline',
    trigger_dag_id='dbt_transformation_pipeline',
    dag=dag
)

# Set task dependencies
test_conn_task >> download_task >> upload_task >> load_task >> trigger_dbt