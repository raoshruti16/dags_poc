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
from airflow.models import Variable

default_args = {
    'owner' : 'shrutirao',
}

INPUT_FILE = '/Users/shrutirao/dags_poc/datasets/Firmographic.csv'
OUTPUT_FOLDER = '/Users/shrutirao/dags_poc/output/'

@task(task_id="read_and_clean", retries=2)
def read_and_clean_data():
    df = pd.read_csv(INPUT_FILE)
    df = df.dropna()

    industries = df['Industry'].unique().tolist()

    Variable.set("industries", json.dumps(industries))

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    return cleaned_data_json

@task(task_id="choose", retries=2)
def choose_industry():
    industries = json.loads(Variable.get('industries'))

    selected_industry = random.choice(industries)

    Variable.set("selected_industry", selected_industry)
    
    return selected_industry

@task.branch(task_id="branch", retries=2)
def determine_branch(**kwargs):
    selected_industry = kwargs['ti'].xcom_pull(task_ids='choose')

    if selected_industry == 'Technology':
        return 'filter_technology'
    elif selected_industry == 'Consulting':
        return 'filter_consulting'
    elif selected_industry == 'Manufacturing':
        return 'filter_manufacturing'
    elif selected_industry == 'Engineering':
        return 'filter_engineering'

def filter_industry(cleaned_data_json: str):
    selected_industry = Variable.get('selected_industry')
    
    cleaned_data = json.loads(cleaned_data_json)
    filtered_data = [row for row in cleaned_data if row['Industry'] == selected_industry]
    
    return filtered_data

@task(task_id="filter_technology", retries=2)
def filter_technology(cleaned_data_json: str):
    return filter_industry(cleaned_data_json)

@task(task_id="filter_consulting", retries=2)
def filter_consulting(cleaned_data_json: str):
    return filter_industry(cleaned_data_json)

@task(task_id="filter_manufacturing", retries=2)
def filter_manufacturing(cleaned_data_json: str):
    return filter_industry(cleaned_data_json)

@task(task_id="filter_engineering", retries=2)
def filter_engineering(cleaned_data_json: str):
    return filter_industry(cleaned_data_json)


@task(task_id="write", retries=2)
def write_to_file(filtered_data: list):
    selected_industry = Variable.get('selected_industry')

    if not filtered_data:
        filtered_data = []

    df = pd.DataFrame(filtered_data)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_file = os.path.join(OUTPUT_FOLDER, f"{selected_industry.lower()}_data.csv")
    
    df.to_csv(output_file, index=False)


@dag(dag_id='taskflow_pipeline_complex_api',
     description = 'Showcases the use of the Taskflow API',
     default_args = default_args,
     start_date = days_ago(1),
     schedule_interval = '@once')
def taskflow_api_dag():
    clear_output_folder = BashOperator(
        task_id='clear_output_folder',
        bash_command='rm -f /Users/shrutirao/dags_poc/output/*'
    )

    clean_data = read_and_clean_data()
    selected_industry = choose_industry()

    clear_output_folder >> clean_data >> selected_industry

    branch_op = determine_branch()

    selected_industry >> branch_op

    branch_op >> [
        write_to_file(filter_technology(clean_data)),
        write_to_file(filter_consulting(clean_data)),
        write_to_file(filter_manufacturing(clean_data)),
        write_to_file(filter_engineering(clean_data))
    ] 

dag = taskflow_api_dag()