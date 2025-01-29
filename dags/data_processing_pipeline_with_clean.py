import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'airflow',
}

def remove_null_values():
    df = pd.read_csv('/Users/shrutirao/dags_poc/datasets/ecommerce_marketing.csv')
    
    df = df.dropna()

    print(df)
    
    df.to_csv('/Users/shrutirao/dags_poc/datasets/ecommerce_marketing_cleaned.csv', index=False)


with DAG(
    dag_id='complex_data_pipeline_clean',
    description = 'Data processing pipeline with multiple operators and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    check_file_exists = BashOperator(
        task_id = 'check_file_exists',
        bash_command = 'test -f /Users/shrutirao/dags_poc/datasets/ecommerce_marketing.csv || exit 1'
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=remove_null_values
    )


check_file_exists.set_downstream(clean_data)
