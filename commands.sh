# In an incognito window login to loony.test.001@gmail.com

# Show that you are on the gmail account page

# Click on your account icon on the top-right -> Manage your account

# Click on "Security" on the left


# Here show that 2-step verification is turned on

# Search for "app passwords" in your google account

# Click on the search result you will go to this page

https://myaccount.google.com/apppasswords


# name: airflow-sla

# Select Generate.
fqgu fcsf wokj hjus

# Copy over the 16 digit password

# Click Done

-------------------------------------------------------------
# Open up airflow.cfg in Sublimetext and add these settings

# Search for the [smtp] section

# Replace the existing contents


smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = loony.test.001@gmail.com
smtp_password = 16_DIGIT_APP_PASSWORD
smtp_port = 587
smtp_mail_from = loony.test.001@gmail.com
smtp_timeout = 30
smtp_retry_limit = 5


# Search for check_slas  and make sure that it is true

check_slas = True

# IMPORTANT: Stop and restart the "scheduler" and "webserver" in the terminal

$ airflow scheduler

$ airflow webserver

-------------------------------------------------------------
# Notes
# SLA for tasks is counter intuitive
# You would expect that, by setting a SLA for a task, you’re defining the expected duration for that task. However, instead of using the start time of the task, Airflow uses the start time of the DAG.
# An SLA, or a Service Level Agreement, is an expectation for the maximum time a Task should be completed relative to the Dag Run start time. If a task takes longer than this to run, it is then visible in the “SLA Misses” part of the user interface, as well as going out in an email of all tasks that missed their SLA.

# On the terminal 

> cd ~/airflow/

> mkdir database

> cd database

> touch my_sqlite.db

> sqlite3 my_sqlite.db


# On the Airflow UI

# Go to Admin -> Connections and show the my_sqlite_conn (should already be set up)

Conn Id: my_sqlite_conn
Conn Type: sqlite
Host: /Users/loonycorn/airflow/database/my_sqlite.db

# Original dataset:
# Dataset: Essentials /Marketing - recordid_name_gender_age_location_email_phone_product_category_amount.csv


# data_processing_pipeline_slas_v1.py
import pandas as pd
import sqlite3
import json
import logging

from time import sleep
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator

from airflow.hooks.base_hook import BaseHook

ORIGINAL_DATA = '/Users/loonycorn/airflow/datasets/ecommerce_marketing.csv'

SQLITE_CONN_ID = 'my_sqlite_conn'

default_args = {
    'owner': 'loonycorn',
    'depends_on_past': False,
    'email': ['cloud.user@loonycorn.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

logger = logging.getLogger(__name__)

def sla_missed_action(*args, **kwargs):
  logger.info('*********************************************************')
  logger.info('**************************WARNING************************')
  logger.info('                   Task Level SLA Missed!                ')
  logger.info('*********************************************************')


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

    sleep(30)

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
    dag_id='monitor_maintain_pipeline_slas',
    description='Pipeline with SLA',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 */6 * * *',
    sla_miss_callback = sla_missed_action
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
        python_callable=aggregate_data,
        sla = timedelta(seconds=10)
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

# Go to the Airflow UI

# In the Grid view

# Unpause the DAG

# Wait for it to run through TILL THE END 2-3 times (it will run every 6 hours)

# PLEASE NOTE THE SELECTIONS BELOW CAREFULLY

# On the left select one run (vertical selection)

# Look at the "Details" and see how long the run took

# Select the "aggregate_data" task for only that one run (horizontal and vertical selection)

# Show that this takes 30+ seconds to run

# Select the entire "aggregate_data" task (horizontal data tasks)

# The "Details" tab should show you a bar graph of how long the tasks takes


# From the navigation bar

Goto "Browse" -> "SLA Misses"

# Observe there are multiple found for SLA Misses (all of them triggered because of transform_data)

# Log in to cloud.user@loonycorn.com

# Show the email that was received for SLA misses

# Open the log file in Sublimetext

# Open Finder window

# > Now goto "$AIRFLOW_HOME/logs/scheduler/[--DATE--]/data_processing_pipeline_slas.py.log"


Search for "**WARNING**"

# Observe this was successfully logged

# Pause the monitor_maintain_pipeline_slas

-------------------------------------------------------------
# Notes

# SLA only works for scheduled DAGs
# If your DAG has no schedule, SLAs will not work. Also, even if your DAG has a schedule but you trigger it manually, Airflow will ignore your run’s start_date and pretend as if your run was scheduled. This happens because Airflow only considers the schedule execution time of each DAG run and the interval between runs. If your DAG does not have an interval between runs, meaning there is no schedule, Airflow fails to calculate the execution dates, leading it to miss any triggered runs. Manually-triggered tasks and tasks in event-driven DAGs will not be checked for an SLA miss.
 
# We schedule our DAG to run every 6 hours

# IMPORTANT: Delete the old DAG run from the UI so you start on a fresh slate

# data_processing_pipeline_slas_v2.py

import pandas as pd
import sqlite3
import json
import logging
import random

from time import sleep
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator

from airflow.hooks.base_hook import BaseHook

ORIGINAL_DATA = '/Users/loonycorn/airflow/datasets/ecommerce_marketing.csv'

SQLITE_CONN_ID = 'my_sqlite_conn'

default_args = {
    'owner': 'loonycorn',
    'depends_on_past': False,
    'email': ['cloud.user@loonycorn.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(seconds=30)
}

logger = logging.getLogger(__name__)

def sla_missed_action(dag, task_list, blocking_task_list, slas, blocking_tis):
  logger.info('*********************************************************')
  logger.info('***********************---WARNING---*********************')
  logger.info('                   Task Level SLA Missed!                ')
  logger.info(f"""
        ---dag: {dag}
        ---task_list: {task_list}
        ---blocking_task_list: {blocking_task_list}
        ---slas: {slas}
        ---blocking_tis: {blocking_tis}
  """)

  logger.info('*********************************************************')


def remove_null_values(**kwargs):
    df = pd.read_csv(ORIGINAL_DATA)
    
    df = df.dropna()

    ti = kwargs['ti']

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    sleep(random.randint(1, 20))

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

    sleep(random.randint(1, 20))

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

    sleep(random.randint(1, 20))


with DAG(
    dag_id='monitor_maintain_pipeline_slas',
    description='Pipeline with SLA',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 */6 * * *',
    sla_miss_callback = sla_missed_action
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
        python_callable=aggregate_data,
        sla = timedelta(seconds=10)
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




# Go to the Airflow UI

# In the Grid view

# Unpause the DAG


# Wait for it to run through TILL THE END 2-3 times (it will run every 6 hours)

# PLEASE NOTE THE SELECTIONS BELOW CAREFULLY

# On the left select one run (vertical selection)

# Look at the "Details" and see how long the run took

# Select the different tasks on the left (horizontal selections)

# The "Details" tab should show you a bar graph of how long each run of the task took

# From the navigation bar

Goto "Browse" -> "SLA Misses"

# Observe there are multiple found for SLA Misses (they're triggered because of different tasks)

# Log in to cloud.user@loonycorn.com

# Show the email that was received for SLA misses

# Open the log file in Sublimetext

# > Now goto "$AIRFLOW_HOME/logs/scheduler/[--DATE--]/monitor_maintain_pipeline.py.log"

Search for "--WARNING-"

# Observe this was successfully logged

# Pause the monitor_maintain_pipeline

-------------------------------------------------------------

# IMPORTANT: Delete the old DAG run from the UI so you start on a fresh slate


# Wire up an on_failed_callback which uses the email operator to send an email

# Randomly fail the tasks by throwing an exception

import pandas as pd
import sqlite3
import json
import logging
import random

from time import sleep
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator

from airflow.hooks.base_hook import BaseHook

from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email


ORIGINAL_DATA = '/Users/loonycorn/airflow/datasets/ecommerce_marketing.csv'

SQLITE_CONN_ID = 'my_sqlite_conn'

default_args = {
    'owner': 'loonycorn',
    'depends_on_past': False,
    'email': ['cloud.user@loonycorn.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'sla': timedelta(seconds=30)
}

logger = logging.getLogger(__name__)

def sla_missed_action(dag, task_list, blocking_task_list, slas, blocking_tis):
  logger.info('*********************************************************')
  logger.info('***********************---WARNING---*********************')
  logger.info('                   Task Level SLA Missed!                ')
  logger.info(f"""
        ---dag: {dag}
        ---task_list: {task_list}
        ---blocking_task_list: {blocking_task_list}
        ---slas: {slas}
        ---blocking_tis: {blocking_tis}
  """)

  logger.info('*********************************************************')

def task_failed_action(context):
    logger.info(f"""
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution Date: {context['task_instance'].execution_date}
    """)

    subject = f"Failed for Task: {context['task_instance'].task_id}"

    body = f"""
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution Date: {context['task_instance'].execution_date}
    """

    email_operator = EmailOperator(
        task_id='send_email',
        to=['cloud.user@loonycorn.com'],
        subject=subject,
        html_content=body,
        mime_subtype='html'
    )
    email_operator.execute(context)

def remove_null_values(**kwargs):
    df = pd.read_csv(ORIGINAL_DATA)
    
    df = df.dropna()

    ti = kwargs['ti']

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    if random.random() < 0.7:
        raise Exception("Intentional task failure")

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

    sleep(random.randint(1, 5))

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
    dag_id='monitor_maintain_pipeline_slas',
    description='Pipeline with SLA',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    sla_miss_callback = sla_missed_action,
    on_failure_callback = task_failed_action
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
        python_callable=aggregate_data,
        sla = timedelta(seconds=10)
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

# Go to the Airflow UI 

# In the Grid view

# Unpause the DAG

# Trigger it manually one at a time

# IMPORTANT
# Wait for it to run through till there is at least 2-3 failures (it will run every minute)

# While a task that fails is tunning the clean_data task will take very long to run (since it is retrying) (3 mins)

# Click on that DAG execution on the left (vertical selection)

# Go to the graph and show that it is being retried (The status on the graph will say up for retry)



# Log in to cloud.user@loonycorn.com

# Show the email that was received for task failure

# There may or may not be an email for SLA miss (I've removed a lot of sleeps)

# Also show that we receive an email for task failure as well

# Pause the pipeline























