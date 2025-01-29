import json
from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the DAG
with DAG(
    'joke_api_dag_xcomm',
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
        do_xcom_push=True,  # Push the result to XCom for further processing
    )

# Function to process the joke response

def process_joke(ti):
    joke_response = ti.xcom_pull(task_ids='get_joke')  # Pull the response from XCom
    joke_response = json.loads(joke_response)  # Parse the JSON response
    
    if 'joke' in joke_response:
        joke = joke_response['joke']
    elif 'setup' in joke_response and 'delivery' in joke_response:
        joke = f"{joke_response['setup']} - {joke_response['delivery']}"
    else:
        joke = "No joke found in response."
    
    print(f"Joke: {joke}")  # Print the joke


# Task to process the joke
process_joke_task = PythonOperator(
    task_id='process_joke',
    python_callable=process_joke,
)

# Define task dependencies
get_joke >> process_joke_task