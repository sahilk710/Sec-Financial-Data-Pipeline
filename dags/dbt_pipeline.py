from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    'dbt_transformation_pipeline',
    default_args=default_args,
    description='DBT transformations for SEC financial data',
    schedule_interval=None,
    catchup=False
)

# DBT tasks
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f'pip3 install --quiet dbt-snowflake==1.5.0 && export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt run --models staging.* --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

dbt_run_facts = BashOperator(
    task_id='dbt_run_facts',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt run --models +fact_balance_sheet fact_cashflow fact_income_statement --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag
)

# Set task dependencies
dbt_deps >> dbt_run_staging >> dbt_run_facts >> dbt_test >> dbt_docs