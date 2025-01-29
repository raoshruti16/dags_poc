import logging
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger('airflow.task')

def extract_sales_data():
    try:
        logger.info("Starting data extraction")
        sales_data = pd.read_csv('datasets/Transactions.csv')
        logger.info(f"Data extraction completed, {len(sales_data)} rows extracted")
        return sales_data
    except Exception as e:
        logger.error("Error during data extraction", exc_info=True)
        raise

def transform_sales_data(ti):
    try:
        logger.info("Starting data transformation")
        sales_data = ti.xcom_pull(task_ids='extract_sales_data')
        sales_data['total'] = sales_data['quantity'] * sales_data['price']
        transformed_data_path = 'transformed_sales_data.csv'
        sales_data.to_csv(transformed_data_path, index=False)
        logger.info(f"Data transformation completed, output saved to {transformed_data_path}")
        return transformed_data_path
    except Exception as e:
        logger.error("Error during data transformation", exc_info=True)
        raise

def load_sales_data(ti):
    try:
        logger.info("Starting data loading")
        transformed_data_path = ti.xcom_pull(task_ids='transform_sales_data')
        transformed_data = pd.read_csv(transformed_data_path)
        logger.info("Data loaded successfully")
        print(transformed_data.head())
    except Exception as e:
        logger.error("Error during data loading", exc_info=True)
        raise

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
    'sales_etl_monitoring_debugging',
    default_args=default_args,
    description='A sales ETL DAG with monitoring and debugging',
    schedule_interval=timedelta(days=1),
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