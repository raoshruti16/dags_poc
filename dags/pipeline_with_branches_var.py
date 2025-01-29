import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import glob

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


default_args = {
   'owner': 'shrutirao'
}

DATASETS_PATH = '/Users/shrutirao/dags_poc/source_data/shares_*.csv'
OUTPUT_PATH = '/Users/shrutirao/dags_poc/output_data/'

def read_csv_files():
    combined_df = pd.DataFrame()
    
    for file in glob.glob(DATASETS_PATH):
        df = pd.read_csv(file)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    return combined_df.to_json(orient='records')

def removing_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_files')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()
    
    return df.to_json()    

def determine_branch():
    action_to_perform = Variable.get("action_to_perform", default_var=None)
    company_to_filter_by = Variable.get("company_to_filter_by", default_var=None)
    
    # Return the task id of the task to execute next
    if action_to_perform == 'filter':
         return f'filter_by_{company_to_filter_by.lower()}'
    elif action_to_perform == 'groupby_ticker':
        return 'groupby_ticker'

def filter_by_ticker(**kwargs):
    ticker = kwargs['ticker']
    json_data = kwargs['ti'].xcom_pull(task_ids='removing_null_values')
    
    df = pd.read_json(json_data)
    
    filtered_df = df[df['Symbol'] == ticker]
    
    output_file = os.path.join(OUTPUT_PATH, f"{ticker.lower()}.csv")
    filtered_df.to_csv(output_file, index=False)

def groupby_ticker(**kwargs):
    json_data = kwargs['ti'].xcom_pull(task_ids='removing_null_values')
    
    df = pd.read_json(json_data)
    
    grouped_df = df.groupby('Symbol').agg({
        'Open': 'mean',
        'High': 'mean',
        'Low': 'mean',
        'Close': 'mean',
        'Volume': 'mean'
    }).reset_index()
    
    output_file = os.path.join(OUTPUT_PATH, "grouped_by_ticker.csv")
    grouped_df.to_csv(output_file, index=False)

with DAG(
    dag_id = 'data_pipeline_with_branches_variable',
    description = 'A data pipeline that uses branching operations',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:

    start = DummyOperator(
        task_id='start'
    )
    
    end = DummyOperator(
        task_id='end'
    )

    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = DATASETS_PATH,
        poke_interval = 10,
        timeout = 60 * 10
    )

    read_csv_files = PythonOperator(
        task_id='read_csv_files',
        python_callable=read_csv_files
    )
    
    removing_null_values = PythonOperator(
        task_id='removing_null_values',
        python_callable=removing_null_values
    )
    
    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    filter_by_aapl = PythonOperator(
        task_id='filter_by_aapl',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'AAPL'}
    )
    
    filter_by_googl = PythonOperator(
        task_id='filter_by_googl',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'GOOGL'}
    )
    
    filter_by_amzn = PythonOperator(
        task_id='filter_by_amzn',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'AMZN'}
    )
    
    groupby_ticker = PythonOperator(
        task_id='groupby_ticker',
        python_callable=groupby_ticker
    )
    
    delete_files = BashOperator(
        task_id='delete_files',
        bash_command='rm -rf /Users/shrutirao/dags_poc/source_data/*.csv',
        trigger_rule='one_success'
    )


start >> checking_for_file >> read_csv_files >> removing_null_values >> determine_branch

determine_branch >> \
    [filter_by_aapl, filter_by_googl, filter_by_amzn, groupby_ticker] >> \
    delete_files >> end



