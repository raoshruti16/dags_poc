import pandas as pd
import sqlite3
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator

from airflow.hooks.base_hook import BaseHook

ORIGINAL_DATA = '/Users/shrutirao/dags_poc/datasets/ecommerce_marketing.csv'

SQLITE_CONN_ID = 'my_sqlite_conn'

default_args = {
    'owner' : 'shrutirao',
}

def remove_null_values(**kwargs):
    df = pd.read_csv(ORIGINAL_DATA)
    
    df = df.dropna()

    ti = kwargs['ti']

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    ti.xcom_push(key='cleaned_data', value=cleaned_data_json)

def aggregate_data(**kwargs):
    ti = kwargs['ti']

    cleaned_data_json = ti.xcom_pull(task_ids='clean_data', key='cleaned_data')
    cleaned_data_dict = json.loads(cleaned_data_json)

    df = pd.DataFrame(cleaned_data_dict)
    
    aggregated_df = df.groupby(['Gender', 'Product', 'Category'])['Amount'].mean().reset_index()
    
    aggregated_df = aggregated_df.sort_values(by='Amount', ascending=False)
    
    aggregated_data_dict = aggregated_df.to_dict(orient='records')
    aggregated_data_json = json.dumps(aggregated_data_dict)

    ti.xcom_push(key='aggregated_data', value=aggregated_data_json)

def insert_into_sqlite(**kwargs):
    ti = kwargs['ti']

    aggregated_data_json = ti.xcom_pull(task_ids='aggregate_data', key='aggregated_data')
    aggregated_data_dict = json.loads(aggregated_data_json)

    connection = BaseHook.get_connection(SQLITE_CONN_ID)

    conn = sqlite3.connect(connection.host)

    cursor = conn.cursor()

    for row in aggregated_data_dict:
        cursor.execute(
            "INSERT INTO aggregated_ecommerce_data (Gender, Product, Category, AvgAmount) VALUES (?, ?, ?, ?)",
            (row['Gender'], row['Product'], row['Category'], row['Amount'])
        )
    
    conn.commit()
    conn.close()


with DAG(
    dag_id='complex_data_pipeline_xcoms',
    description = 'Data processing pipeline with multiple operators and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    check_file_exists = BashOperator(
        task_id = 'check_file_exists',
        bash_command = f'test -f {ORIGINAL_DATA} || exit 1'
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=remove_null_values
    )


    aggregate_data = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data
    )

    drop_table_if_exists = SqliteOperator(
        task_id='drop_table_if_exists',
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS aggregated_ecommerce_data;
        """
    )

    create_table = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
            CREATE TABLE aggregated_ecommerce_data (
                Gender TEXT,
                Product TEXT,
                Category TEXT,
                AvgAmount FLOAT
            )
        """
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=insert_into_sqlite
    )

check_file_exists >> clean_data >> aggregate_data >> drop_table_if_exists >> create_table >> load_data
