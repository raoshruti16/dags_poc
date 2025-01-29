from datetime import datetime, timedelta
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.hooks.S3_hook import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

default_args = {
    'owner': 'loonycorn'
}

def upload_file_to_s3():
    s3_hook = S3Hook(aws_conn_id='my_aws_conn')
    s3_hook.load_file(
        filename='/Users/loonycorn/airflow/datasets/insurance_fraud_prevention.csv',
        key='insurance_fraud_prevention.csv',
        bucket_name='loony-airflow-bucket',
        replace=True
    )

def transfer_file_from_s3_to_gcs():
    s3_hook = S3Hook(aws_conn_id='my_aws_conn')
    gcs_hook = GCSHook(gcp_conn_id='my_gcp_conn')

    bucket_name = 'loony-airflow-bucket'
    object_name = 'insurance_fraud_prevention.csv'
    
    file_content = s3_hook.read_key(key=object_name, bucket_name=bucket_name)

    gcs_hook.upload(
        bucket_name='loony-insurance-bucket',
        object_name=object_name,
        data=file_content
    )

def create_bigquery_dataset():
    hook = BigQueryHook(gcp_conn_id='my_gcp_conn')

    hook.run_with_configuration({
        "query": {
            "query": "CREATE SCHEMA IF NOT EXISTS `ifp_dataset`",
            "useLegacySql": False
        }
    })

def create_bigquery_external_table():
    hook = BigQueryHook(gcp_conn_id='my_gcp_conn')
    hook.run_with_configuration({
        "query": {
            "query": """
                CREATE OR REPLACE EXTERNAL TABLE `ifp_dataset.ifp_table`
                (
                    claim_id INT64,
                    insured_name STRING,
                    claim_amount FLOAT64,
                    claim_date DATE,
                    claim_type STRING,
                    investigation_status STRING
                )
                OPTIONS (
                    format = 'CSV',
                    uris = ['gs://loony-insurance-bucket/insurance_fraud_prevention.csv'],
                    skip_leading_rows = 1
                )
            """,
            "useLegacySql": False
        }
    })


with DAG(
    dag_id='integrating_with_external_services',
    description='A data pipeline that integrates with external services',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once'
) as dag:

    upload_file_to_s3_task = PythonOperator(
        task_id='upload_file_to_s3',
        python_callable=upload_file_to_s3
    )

    transfer_file_from_s3_to_gcs_task = PythonOperator(
        task_id='transfer_file_from_s3_to_gcs',
        python_callable=transfer_file_from_s3_to_gcs
    )

    create_bigquery_dataset_task = PythonOperator(
        task_id='create_bigquery_dataset',
        python_callable=create_bigquery_dataset
    )

    create_bigquery_external_table_task = PythonOperator(
        task_id='create_bigquery_external_table',
        python_callable=create_bigquery_external_table
    )


    upload_file_to_s3_task >> transfer_file_from_s3_to_gcs_task >> \
    create_bigquery_dataset_task >> create_bigquery_external_table_task


