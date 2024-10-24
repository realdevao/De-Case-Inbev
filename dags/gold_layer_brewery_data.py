import pandas as pd
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.exceptions import AirflowFailException
from datetime import timedelta

# Defining the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=60),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['ops_team@inbev.com'],
    'execution_timeout': timedelta(minutes=20),
}

# Defining the paths for the Layers through Airflow Variables
silver_layer_path = Variable.get("silver_layer_path", "/opt/airflow/silver_layer/breweries_partitioned")
gold_layer_path = Variable.get("gold_layer_path", "/opt/airflow/gold_layer/breweries_aggregated.parquet")

# Function to send custom notifications in case of failure
def notify_failure(context):
    subject = f"DAG {context['task_instance'].dag_id} Failed"
    body = f"""
    Task: {context['task_instance'].task_id} <br>
    DAG: {context['task_instance'].dag_id} <br>
    Execution Time: {context['execution_date']} <br>
    Log URL: {context['task_instance'].log_url}
    """
    send_email(to=context['task_instance'].email, subject=subject, html_content=body)

# Function for data aggregation in the Gold Layer
def aggregate_data(**kwargs):
    try:
        # Load data from the Silver Layer
        silver_file_path = silver_layer_path
        if not os.path.exists(silver_file_path):
            raise AirflowFailException(f"Silver Layer data not found at {silver_file_path}")

        df = pd.read_parquet(silver_file_path)

        # Checking if the DataFrame contains data
        if df.empty:
            raise AirflowFailException("No data found in Silver Layer file")

        # Counting breweries by type and state_province
        agg_df = df.groupby(['state_province', 'brewery_type']).size().reset_index(name='brewery_count')

        # Saving the aggregated data temporarily
        temp_parquet_path = '/tmp/breweries_aggregated_temp.parquet'
        agg_df.to_parquet(temp_parquet_path)

        # Moving the data to the Gold Layer
        final_parquet_path = gold_layer_path
        os.makedirs(os.path.dirname(final_parquet_path), exist_ok=True)
        os.replace(temp_parquet_path, final_parquet_path)

        logging.info(f"Aggregated data successfully saved to {final_parquet_path}")

    except Exception as e:
        logging.error(f"Error aggregating data: {e}")
        raise AirflowFailException(f"Data aggregation failed: {e}")


# Defining the DAG for the Gold Layer
with DAG(
    'gold_layer_aggregate_data',
    default_args=default_args,
    description='Data aggregation for the Gold Layer',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=notify_failure
) as dag:

    task_aggregate_data = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        provide_context=True
    )

    task_aggregate_data
