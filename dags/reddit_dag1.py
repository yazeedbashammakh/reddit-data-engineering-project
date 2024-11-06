import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.reddit_to_gcs import fetch_subreddit_to_gcs
from tasks.bigquery_processing import load_to_bq_staging


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
}

# Define the DAG
with DAG(
    dag_id='reddit_data_fetch',
    default_args=default_args,
    description='This DAG pulls data from reddit to GCP',
    schedule_interval='@daily',
    start_date=datetime(2024, 10, 1),
    catchup=False
) as dag:

    # Define Task 1: Fetch Data from reddit to GCS
    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_subreddit_to_gcs,
        provide_context=True,
        op_kwargs={'subreddit_name': 'dataengineering'}
    )

    # Define Task 2: load data into staging BQ table
    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=load_to_bq_staging,
        provide_context=True,
        op_kwargs={
            'data_name': 'posts'
        }
    )

    # Set task dependencies
    fetch_data_task >> process_data_task
