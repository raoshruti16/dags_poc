from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'airflow',
}

with DAG(
    dag_id='complex_data_pipeline',
    description = 'Data processing pipeline with multiple operators and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    check_file_exists = BashOperator(
        task_id = 'check_file_exists',
        bash_command = 'test -f /Users/shrutirao/dags_poc/datasets/ecommerce_marketing.csv || exit 1'
    )

check_file_exists