import json
from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the DAG
with DAG(
    'joke_api_dag',
    default_args=default_args,
    description='A simple DAG to call JokeAPI',
    schedule_interval='@daily',  # Schedule to run daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # Task to get a joke from the JokeAPI
    get_joke = SimpleHttpOperator(
        task_id='get_joke',
        method='GET',
        http_conn_id='joke_api',  # Connection ID defined in Airflow UI
        headers={"Content-Type": "application/json"},
        endpoint='',  # Empty because the full URL is already defined in the connection
    )