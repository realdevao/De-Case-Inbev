from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.utils.email import send_email
from airflow.exceptions import AirflowFailException

from datetime import timedelta
import requests
import json
import os
import logging

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
    'execution_timeout': timedelta(minutes=10),
}

# Setting the Bronze Layer path from an Airflow variable
bronze_layer_path = Variable.get("bronze_layer_path", "/opt/airflow/bronze_layer/")

# Function to send custom notifications
def notify_failure(context):
    subject = f"DAG {context['task_instance'].dag_id} Failed"
    body = f"""
    Task: {context['task_instance'].task_id} <br>
    DAG: {context['task_instance'].dag_id} <br>
    Execution Time: {context['execution_date']} <br>
    Log URL: {context['task_instance'].log_url}
    """
    send_email(to=context['task_instance'].email, subject=subject, html_content=body)

# Function to make the API request and save the data
def fetch_brewery_data(**kwargs):
    url = 'https://api.openbrewerydb.org/breweries'
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Creating a secure temporary directory for initial storage
        temp_file_path = '/tmp/breweries_raw.json'
        with open(temp_file_path, 'w') as temp_file:
            json.dump(data, temp_file)

        # Moving the file to the final destination only after success
        final_file_path = os.path.join(bronze_layer_path, 'breweries_raw.json')
        os.makedirs(os.path.dirname(final_file_path), exist_ok=True)
        os.replace(temp_file_path, final_file_path)
        logging.info(f"Data successfully saved to {final_file_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        raise AirflowFailException(f"API request failed: {e}")

# Defining the DAG
with DAG(
    'bronze_layer_brewery_data',
    default_args=default_args,
    description='Production pipeline for ingesting Open Brewery DB data into the Bronze Layer',
    schedule_interval='@daily',  # Pode ser ajustado para horários de menor tráfego
    start_date=days_ago(1),  # Definindo para rodar no dia anterior (evitando catchup longo)
    catchup=False,  # Para evitar execuções passadas atrasadas
    max_active_runs=1,  # Limitar uma execução ativa por vez
    on_failure_callback=notify_failure  # Enviando notificação de falha
) as dag:

    task_fetch_data = PythonOperator(
        task_id='fetch_brewery_data',
        python_callable=fetch_brewery_data,
        provide_context=True  # Passando informações de contexto da DAG para o Python callable
    )

    task_fetch_data
