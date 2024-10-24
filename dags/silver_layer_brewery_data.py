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
bronze_layer_path = Variable.get("bronze_layer_path", "/opt/airflow/bronze_layer/breweries_raw.json")
silver_layer_path = Variable.get("silver_layer_path", "/opt/airflow/silver_layer/breweries_partitioned")

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

# Function to transform the data from the Bronze Layer and partition it
def transform_data(**kwargs):
    try:
        # Loading data from the Bronze Layer
        bronze_file_path = bronze_layer_path
        if not os.path.exists(bronze_file_path):
            raise AirflowFailException(f"Bronze Layer data not found at {bronze_file_path}")

        df = pd.read_json(bronze_file_path)

        # Checking if the DataFrame contains data
        if df.empty:
            raise AirflowFailException("No data found in Bronze Layer file")

        # Removing records without state
        df.dropna(subset=['state'], inplace=True)

        # Saving the transformed data temporarily to avoid inconsistencies
        temp_parquet_path = '/tmp/breweries_partitioned_temp'
        df.to_parquet(temp_parquet_path, partition_cols=['state'])

        # Moving the partitioned files to the final directory in the Silver Layer
        final_parquet_path = silver_layer_path
        os.makedirs(final_parquet_path, exist_ok=True)

        # Removing old files before moving the new ones
        for root, dirs, files in os.walk(final_parquet_path):
            for file in files:
                os.remove(os.path.join(root, file))

        # Moving files from the temporary directory to the final destination
        for dirpath, dirs, files in os.walk(temp_parquet_path):
            for file in files:
                temp_file_path = os.path.join(dirpath, file)
                partition_name = os.path.basename(dirpath)
                final_file_path = os.path.join(final_parquet_path, partition_name, file)
                os.makedirs(os.path.dirname(final_file_path), exist_ok=True)
                os.replace(temp_file_path, final_file_path)

        # Log to check the result
        logging.info(f"Files in Silver Layer after move: {os.listdir(final_parquet_path)}")

        logging.info(f"Data successfully transformed and saved to {final_parquet_path}")

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise AirflowFailException(f"Data transformation failed: {e}")

# Defining the DAG for the Silver Layer
with DAG(
    'silver_layer_transform_data',
    default_args=default_args,
    description='Transformação e particionamento de dados para a Silver Layer',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=notify_failure
) as dag:

    task_transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    task_transform_data
