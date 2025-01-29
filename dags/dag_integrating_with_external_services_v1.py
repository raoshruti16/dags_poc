from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG

from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator
)

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

default_args = {
   'owner': 'loonycorn'
}

with DAG(
    dag_id = 'integrating_with_external_services',
    description = 'A data pipeline that integrates with external services',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:

    create_bucket = GCSCreateBucketOperator(
        task_id = 'create_bucket',
        bucket_name = 'loony-insurance-bucket',
        gcp_conn_id = 'my_gcp_conn',
    )


    upload_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_file_to_gcs',
        src='/Users/loonycorn/airflow/datasets/insurance_fraud_prevention.csv',
        dst='insurance_fraud_prevention.csv',
        bucket='loony-insurance-bucket',
        gcp_conn_id='my_gcp_conn',
    )

create_bucket >> upload_file_to_gcs