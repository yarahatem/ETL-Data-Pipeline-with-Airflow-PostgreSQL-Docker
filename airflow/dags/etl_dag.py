from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts folder to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/../scripts'))
from etl_script import extract, transform, load

# Default DAG settings
default_args = {
    'owner': 'yara',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
with DAG(
    'crypto_etl_dag',
    default_args=default_args,
    description='ETL pipeline: Extract, Transform, Load into PostgreSQL',
    schedule_interval=timedelta(minutes=5),  # every 5 min
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True
    )

    # Set task order
    extract_task >> transform_task >> load_task
