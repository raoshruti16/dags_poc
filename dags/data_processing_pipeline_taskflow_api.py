import os
import pandas as pd
import random
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'shrutirao',
}

INPUT_FILE = '/Users/shrutirao/dags_poc/datasets/Firmographic.csv'
OUTPUT_FOLDER = '/Users/shrutirao/dags_poc/output/'

@task
def read_and_clean_data():
    df = pd.read_csv(INPUT_FILE)
    df = df.dropna()

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    return df.to_json()

@task
def groupby_industry(cleaned_data_json: str):
    df = pd.read_json(cleaned_data_json)

    grouped_data = df.groupby("Industry").agg({
        'Number of Employees': 'mean',
        'Company Revenue': 'mean'
    }).reset_index()
    
    return grouped_data.to_json()

@task
def write_to_file(grouped_data_json: str):
    df = pd.read_json(grouped_data_json)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_file = os.path.join(OUTPUT_FOLDER, 'grouped_data.csv')
    
    df.to_csv(output_file, index=False)


@dag(dag_id='taskflow_pipeline_api',
     description = 'Showcases the use of the Taskflow API',
     default_args = default_args,
     start_date = days_ago(1),
     schedule_interval = '@once')
def taskflow_api_dag():

    clear_output_folder = BashOperator(
        task_id='clear_output_folder',
        bash_command='rm -f /Users/shrutirao/dags_poc/output/*'
    )


    clear_output_folder >> write_to_file(groupby_industry(read_and_clean_data()))

dag = taskflow_api_dag()





