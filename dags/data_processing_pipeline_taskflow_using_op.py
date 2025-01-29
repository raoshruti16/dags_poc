import os
import pandas as pd
import random
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'shrutirao',
}

INPUT_FILE = '/Users/shrutirao/dags_poc/datasets/Firmographic.csv'
OUTPUT_FOLDER = '/Users/shrutirao/dags_poc/output/'

def read_and_clean_data(**kwargs):
    df = pd.read_csv(INPUT_FILE)
    df = df.dropna()

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    return df.to_json()

def groupby_industry(**kwargs):
    ti = kwargs['ti']

    cleaned_data = ti.xcom_pull(task_ids='read_and_clean_data')
    # cleaned_data = json.loads(cleaned_data)

    df = pd.read_json(cleaned_data)

    grouped_data = df.groupby("Industry").agg({
        'Number of Employees': 'mean',
        'Company Revenue': 'mean'
    }).reset_index()
    
    return grouped_data.to_json()

def write_to_file(**kwargs):
    ti = kwargs['ti']

    grouped_data_json = ti.xcom_pull(task_ids='groupby_industry')

    df = pd.read_json(grouped_data_json)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_file = os.path.join(OUTPUT_FOLDER, 'grouped_data.csv')
    
    df.to_csv(output_file, index=False)



with DAG(
    dag_id='taskflow_pipeline_using_operators',
    description = 'Showcases the use of the Taskflow API',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:
    clear_output_folder = BashOperator(
        task_id='clear_output_folder',
        bash_command='rm -f /Users/shrutirao/dags_poc/output/*'
    )

    read_and_clean_data = PythonOperator(
        task_id='read_and_clean_data',
        python_callable=read_and_clean_data
    )

    groupby_industry = PythonOperator(
        task_id='groupby_industry',
        python_callable=groupby_industry
    )

    write_to_file = PythonOperator(
        task_id='write_to_file',
        python_callable=write_to_file
    )

clear_output_folder >> read_and_clean_data >> groupby_industry >> write_to_file








