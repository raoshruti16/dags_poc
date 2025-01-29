import json
from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Function to process the joke response
def process_joke(ti):
    joke_response = ti.xcom_pull(task_ids='get_joke')  # Pull the response from XCom
    joke_response = json.loads(joke_response)  # Parse the JSON response
    # Extract the joke from the response
    joke = joke_response['joke'] if 'joke' in joke_response else f"{joke_response['setup']} - {joke_response['delivery']}"
    print(f"Joke: {joke}")  # Print the joke

# Define the DAG
with DAG(
    'joke_api_dag_using_api',
    default_args=default_args,
    description='A simple DAG to call JokeAPI',
    schedule_interval='@daily',  # Schedule to run daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # Task to check if the API is available
    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='joke_api',
        endpoint='',  # Empty because the full URL is already defined in the connection
        response_check=lambda response: "joke" in response.text or "setup" in response.text,
        poke_interval=5,  # Check every 5 seconds
        timeout=20,  # Timeout after 20 seconds
    )

    # Task to get a joke from the JokeAPI
    get_joke = SimpleHttpOperator(
        task_id='get_joke',
        method='GET',
        http_conn_id='joke_api',  # Connection ID defined in Airflow UI
        endpoint='',  # Empty because the full URL is already defined in the connection
        headers={"Content-Type": "application/json"},
        do_xcom_push=True,  # Push the result to XCom for further processing
    )

    # Task to process the joke
    process_joke_task = PythonOperator(
        task_id='process_joke',
        python_callable=process_joke,
    )

    # Define task dependencies
    check_api >> get_joke >> process_joke_task