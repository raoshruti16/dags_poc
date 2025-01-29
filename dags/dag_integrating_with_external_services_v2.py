from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator
)

from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator
)

from airflow.operators.dummy import DummyOperator
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


    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bigquery_dataset',
        dataset_id='ifp_dataset',
        gcp_conn_id='my_gcp_conn',
    )

    join = DummyOperator(
        task_id='join'
    )

    create_bigquery_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_bigquery_external_table',
        destination_project_dataset_table='ifp_dataset.ifp_table',
        bucket='loony-insurance-bucket',
        source_objects=['insurance_fraud_prevention.csv'],
        schema_fields=[
            {"name": "claim_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "insured_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "claim_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "claim_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "claim_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "investigation_status", "type": "STRING", "mode": "NULLABLE"}
        ],
        gcp_conn_id='my_gcp_conn',
        source_format='CSV'
    )

[create_bucket >> upload_file_to_gcs] >> join

create_bigquery_dataset >> join

join >> create_bigquery_external_table
