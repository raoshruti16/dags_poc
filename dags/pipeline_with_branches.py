from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
   'owner': 'shrutirao'
}

with DAG(
    dag_id = 'data_pipeline_with_branches',
    description = 'A data pipeline that uses branching operations',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:

    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = '/Users/shrutirao/dags_poc/source_data/shares_1.csv',
        poke_interval = 10,
        timeout = 60 * 10
    )


    checking_for_file
