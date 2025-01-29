import os
import pandas as pd
import random
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    'owner' : 'shrutirao',
}

INPUT_FILE = '/Users/shrutirao/dags_poc/datasets/Firmographic.csv'
OUTPUT_FOLDER = '/Users/shrutirao/dags_poc/output/'

def read_and_clean_data(**kwargs):
    df = pd.read_csv(INPUT_FILE)
    df = df.dropna()

    industries = df['Industry'].unique().tolist()

    Variable.set("industries", json.dumps(industries))

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    return cleaned_data_json

def choose_industry(**kwargs):
    industries = json.loads(Variable.get('industries'))

    selected_industry = random.choice(industries)

    Variable.set("selected_industry", selected_industry)
    
    return selected_industry

def determine_branch(**kwargs):
    selected_industry = kwargs['ti'].xcom_pull(task_ids='choose_industry')

    if selected_industry == 'Technology':
        return 'filter_technology'
    elif selected_industry == 'Consulting':
        return 'filter_consulting'
    elif selected_industry == 'Manufacturing':
        return 'filter_manufacturing'
    elif selected_industry == 'Engineering':
        return 'filter_engineering'

def filter_industry(**kwargs):
    ti = kwargs['ti']

    selected_industry = Variable.get('selected_industry')
    
    cleaned_data = ti.xcom_pull(task_ids='read_and_clean_data')
    cleaned_data = json.loads(cleaned_data)
    filtered_data = [row for row in cleaned_data if row['Industry'] == selected_industry]
    
    return filtered_data

def write_to_file(**kwargs):
    selected_industry = Variable.get('selected_industry')

    ti = kwargs['ti']
    filtered_data = ti.xcom_pull(task_ids=f'filter_{selected_industry.lower()}')

    if not filtered_data:
        filtered_data = []

    df = pd.DataFrame(filtered_data)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_file = os.path.join(OUTPUT_FOLDER, f"{selected_industry.lower()}_data.csv")
    
    df.to_csv(output_file, index=False)



with DAG(
    dag_id='taskflow_pipeline_complex',
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

    choose_industry = PythonOperator(
        task_id='choose_industry',
        python_callable=choose_industry
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )

    filter_technology = PythonOperator(
        task_id='filter_technology',
        op_kwargs={'industry': 'Technology'},
        python_callable=filter_industry,
        do_xcom_push=True
    )

    filter_consulting = PythonOperator(
        task_id='filter_consulting',
        op_kwargs={'industry': 'Consulting'},
        python_callable=filter_industry,
        do_xcom_push=True
    )

    filter_manufacturing = PythonOperator(
        task_id='filter_manufacturing',
        op_kwargs={'industry': 'Manufacturing'},
        python_callable=filter_industry,
        do_xcom_push=True
    )

    filter_engineering = PythonOperator(
        task_id='filter_engineering',
        op_kwargs={'industry': 'Engineering'},
        python_callable=filter_industry,
        do_xcom_push=True
    )

    write_to_file = PythonOperator(
        task_id='write_to_file',
        python_callable=write_to_file,
        trigger_rule='one_success'
    )



clear_output_folder >> read_and_clean_data >> choose_industry >> determine_branch

determine_branch >> filter_technology >> write_to_file
determine_branch >> filter_consulting >> write_to_file
determine_branch >> filter_manufacturing >> write_to_file
determine_branch >> filter_engineering >> write_to_file








