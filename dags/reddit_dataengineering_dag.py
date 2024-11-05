from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.reddit_to_gcs import fetch_subreddit_to_gcs
from tasks.bigquery_processing import load_to_bq_staging, load_to_bq_subreddit_final, load_to_bq_posts_final, load_to_bq_comments_final, last_month_aggregated_metrics


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
}

SUBREDDIT = "dataengineering"

# Define the DAG
with DAG(
    dag_id=f'reddit_data_fetch_{SUBREDDIT}',
    default_args=default_args,
    description='This DAG pulls data from reddit to GCP',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 10, 1)
) as dag:

    # Define Task 1: Fetch Data from reddit to GCS
    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_subreddit_to_gcs,
        provide_context=True,
        op_kwargs={'subreddit_name': SUBREDDIT}
    )

    bq_staging_tasks = {}

    for data_name in ['subreddit', 'posts', 'comments']:

        # Define Task: load data into staging BQ table
        bq_staging_tasks[data_name] = PythonOperator(
            task_id=f'load_{data_name}_bq_stag_task',
            python_callable=load_to_bq_staging,
            provide_context=True,
            op_kwargs={
                'data_name': data_name
            }
        )

    # Define Task: load data into final BQ table subreddit 
    process_subreddit_data_final_task = PythonOperator(
        task_id='process_subreddit_data_final_task',
        python_callable=load_to_bq_subreddit_final,
        provide_context=True,
    )

    # Define Task: load data into final BQ table posts 
    process_posts_data_final_task = PythonOperator(
        task_id='process_posts_data_final_task',
        python_callable=load_to_bq_posts_final,
        provide_context=True,
    )

    # Define Task: load data into final BQ table comments 
    process_comments_data_final_task = PythonOperator(
        task_id='process_comments_data_final_task',
        python_callable=load_to_bq_comments_final,
        provide_context=True,
    )

    # Aggregated table
    last_month_aggregation_task = PythonOperator(
        task_id='last_month_aggregation_task',
        python_callable=last_month_aggregated_metrics,
        provide_context=True,
    )

    # Set task dependencies
    fetch_data_task >> bq_staging_tasks["subreddit"] >> process_subreddit_data_final_task >> last_month_aggregation_task
    fetch_data_task >> bq_staging_tasks["posts"] >> process_posts_data_final_task >> last_month_aggregation_task
    fetch_data_task >> bq_staging_tasks["comments"] >> process_comments_data_final_task >> last_month_aggregation_task
