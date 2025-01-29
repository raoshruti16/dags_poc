from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def extract_sales_data():
    sales_data = pd.read_csv('datasets/Transactions.csv')
    return sales_data

def transform_sales_data(ti):
    sales_data = ti.xcom_pull(task_ids='extract_sales_data')
    sales_data['total'] = sales_data['quantity'] * sales_data['price']
    transformed_data_path = 'transformed_sales_data.csv'
    sales_data.to_csv(transformed_data_path, index=False)
    return transformed_data_path

def load_sales_data(ti):
    transformed_data_path = ti.xcom_pull(task_ids='transform_sales_data')
    transformed_data = pd.read_csv(transformed_data_path)
    print(transformed_data.head())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_etl',
    default_args=default_args,
    description='A simple sales ETL DAG',
    schedule_interval='0 12 * * *',
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_sales_data',
    python_callable=transform_sales_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_sales_data',
    python_callable=load_sales_data,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> extract_task >> transform_task >> load_task >> end
