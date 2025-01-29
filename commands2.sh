# Have two terminal tabs open

# Start on the first one

$ pip install 'apache-airflow[google]'
$ pip install 'apache-airflow[amazon]'

$ airflow scheduler

# On the second tab
$ airflow webserver

# Show that you are logged on on the Airflow UI

--------------------------------------------
# Behind the scenes
# On a second tab set up the GCP console behind the scenes

# Be logged in to the GCP platform "https://console.cloud.google.com/" as cloud.user@loonycorn.com

# Have a project selected (i.e. loony-test-project)

# SHOW IN RECORDING FROM HERE

# Click on the hamburger menu on the top left corner of the console -> View all Products

# Select Google Cloud Storage

# Go to the page and show that there are no buckets

# Click on hamburger -> View All Products -> Open in new tab

# Select IAM.

Click on Service accounts from the left pane to look at the service accounts.
(Should be empty for a new project)

Click on "Create Service Account" to create a new service account.

service account name: loony-airflow-sa
description: Service account to access Google Cloud APIs from Apache Airflow

# Click Next

Bigquery -> BigQuery Admin

Click "Add another Role"

Cloud Storage -> Storage Admin

Click "Continue" -> "Done"

Click on the 3 dots on the right of the service account and click on "Manage Key"

Click "Add Key" -> "Create new key" -> "JSON" -> "Create"

Open and show the JSON file -> Copy this

--------------------------------------------

# Go to the Airflow UI

Click on Admin -> Connections

Click on "Create"

Connection ID: my_gcp_conn
Connection Type: Google Cloud
Keyfile JSON: Paste the JSON file copied

Click on "Test" -> "Save"
# If Test does not work, that is fine, just save

# Original dataset: Insurance/Insurance Fraud Prevention.csv

# dag_integrating_with_external_services_v1.py
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


# Go back to localhost:8080 and trigger the DAG.

# In UI, Once the DAG is triggered, you can check the logs to see if the bucket was created successfully.

# Go to the Google Cloud Console and check if the bucket was created.

# Check the contents of the bucket and you should see the file

# Delete the file from the bucket in preparation for the next part of the demo

------------------------------------------------------

# On GCP

# Click on hamburger -> View All Products -> Open in new tab

# Select BigQuery -> Show that there are no datasets/tables here

# dag_integrating_with_external_services_v2.py

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
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


# Go back to localhost:8080 and go to graph and observe the DAG.

# Trigger the DAG from the UI

# Watch it run through

# NOTE: If your DAG does not load because of an openlineage dependency,
# Stop the scheduler and webserver, run the command below and restart

pip install apache-airflow-providers-openlineage


# Also check if the csv was uploaded to the bucket, 
# check if the dataset and table were created in BigQuery.

# In the BigQuery console, you can see the dataset and table created.

SELECT * FROM `loony-rest-api.ifp_dataset.ifp_table`;

------------------------------------------------------
# Behind the scenes

# On a new tab log in to https://console.aws.amazon.com

# Search "S3"

# "Create bucket"

name: loony-airflow-bucket
region: US East (Ohio) us-east-2

------------------------------------------------------

# Back to recording

# > Search "IAM"

# > Click "Users" > "create"

name: loony-airflow-user

# Click on Attach a policy directly

# Search for "AmazonS3FullAccess"

# Attach this policy to the user

# Create the user

# Click on the new user loony-airflow-user

# Scroll to Access Keys -> Create a new access key -> Others

# Copy over the access key and secret access key
AKIATOKI4M3BOTPLVGYI
q7nYS4lRlyoNjLdHtZbkXqFuTnHdpQPcn0/wKxdv

# Go to the Airflow UI

# From the top navigation bar click "Admin" -> "Connections"

# Click on the + button to create a new connection

name: my_aws_conn
connection type: amazon web services
aws access key: AKIATOKI4M3BOTPLVGYI
aws secrect access key: q7nYS4lRlyoNjLdHtZbkXqFuTnHdpQPcn0/wKxdv

# Test the connection - it should be successful and show our user

# Save the connection

------------------------------------------------------

# Delete the file in GCP bucket (Dont delete the bucket)

# Delete the dataset in BigQuery

# dag_integrating_with_external_services_v3.py


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


# Go back to localhost:8080 and go to graph and observe the DAG

# Trigger the DAG

# Go to the S3 bucket and show the data has been uploaded there

# Go to the GCS bucket and show the data has been transferred there

# Go to the BigQuery console and refresh the dataset to see the table created

# Run a query on the table

SELECT * FROM `loony-rest-api.ifp_dataset.ifp_table` LIMIT 1000

