# Delete all the files in output folder

# Keep the dataset Firmographic.csv in the dataset folder in ~/airflow


# Many operators will auto-push their results into an XCom key called return_value if the do_xcom_push argument is set to True (as it is by default), and @task functions do this as well. xcom_pull defaults to using return_value as key if no key is passed to it, meaning it’s possible to write code like this:

# Note that we pull the data without specifying the keys


# First let us set up a very simple DAG

# data_processing_pipeline_taskflow_v3.py

import os
import pandas as pd
import random
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'loonycorn',
}

INPUT_FILE = '/Users/loonycorn/airflow/datasets/Firmographic.csv'
OUTPUT_FOLDER = '/Users/loonycorn/airflow/output/'

def read_and_clean_data(**kwargs):
    df = pd.read_csv(INPUT_FILE)
    df = df.dropna()

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    return df.to_json()

def groupby_industry(**kwargs):
    ti = kwargs['ti']

    cleaned_data = ti.xcom_pull(task_ids='read_and_clean_data')

    df = pd.read_json(cleaned_data)

    grouped_data = df.groupby("Industry").agg({
        'Number of Employees': 'mean',
        'Company Revenue': 'mean'
    }).reset_index()
    
    return grouped_data.to_json()

def write_to_file(**kwargs):
    ti = kwargs['ti']

    grouped_data_json = ti.xcom_pull(task_ids='groupby_industry')

    df = pd.read_json(grouped_data_json)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_file = os.path.join(OUTPUT_FOLDER, 'grouped_data.csv')
    
    df.to_csv(output_file, index=False)



with DAG(
    dag_id='taskflow_pipeline',
    description = 'Showcases the use of the Taskflow API',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:
    clear_output_folder = BashOperator(
        task_id='clear_output_folder',
        bash_command='rm -f /Users/loonycorn/airflow/output/*'
    )

    read_and_clean_data = PythonOperator(
        task_id='read_and_clean_data',
        python_callable=read_and_clean_data
    )

    groupby_industry = PythonOperator(
        task_id='groupby_industry',
        python_callable=groupby_industry
    )

    write_to_file = PythonOperator(
        task_id='write_to_file',
        python_callable=write_to_file
    )

clear_output_folder >> read_and_clean_data >> groupby_industry >> write_to_file


# Go to the airflow UI

# Show the Grid view

# Show the Graph view

# Run the DAG and toggle on the Auto-refresh

# Once the run is complete

# Show the output folder and the file created


--------------------------------------------------------------------------------

# Let us now use the taskflow API for this

# NOTES
# If you write most of your DAGs using plain Python code rather than Operators, then the TaskFlow API will make it much easier to author clean DAGs without extra boilerplate, all using the @task decorator.

# TaskFlow takes care of moving inputs and outputs between your Tasks using XComs for you, as well as automatically calculating dependencies - when you call a TaskFlow function in your DAG file, rather than executing it, you will get an object representing the XCom for the result (an XComArg), that you can then use as inputs to downstream tasks or operators.

# The purpose of the TaskFlow API in Airflow is to simplify the DAG authoring experience by eliminating the boilerplate code required by traditional operators. The result can be cleaner DAG files that are more concise and easier to read.

# In general, whether you use the TaskFlow API is a matter of your own preference and style. In most cases, a TaskFlow decorator and the corresponding traditional operator will have the same functionality. You can also mix decorators and traditional operators within a single DAG.
# NOTES



# IMPORTANT: While recording please add the import first

from airflow.decorators import task, dag

# And then add each @task function one at a time

# data_processing_pipeline_taskflow_v2.py

import os
import pandas as pd
import random
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'loonycorn',
}

INPUT_FILE = '/Users/loonycorn/airflow/datasets/Firmographic.csv'
OUTPUT_FOLDER = '/Users/loonycorn/airflow/output/'

@task
def read_and_clean_data():
    df = pd.read_csv(INPUT_FILE)
    df = df.dropna()

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    return df.to_json()

@task
def groupby_industry(cleaned_data_json: str):
    df = pd.read_json(cleaned_data_json)

    grouped_data = df.groupby("Industry").agg({
        'Number of Employees': 'mean',
        'Company Revenue': 'mean'
    }).reset_index()
    
    return grouped_data.to_json()

@task
def write_to_file(grouped_data_json: str):
    df = pd.read_json(grouped_data_json)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_file = os.path.join(OUTPUT_FOLDER, 'grouped_data.csv')
    
    df.to_csv(output_file, index=False)


@dag(dag_id='taskflow_pipeline',
     description = 'Showcases the use of the Taskflow API',
     default_args = default_args,
     start_date = days_ago(1),
     schedule_interval = '@once')
def taskflow_api_dag():

    clear_output_folder = BashOperator(
        task_id='clear_output_folder',
        bash_command='rm -f /Users/loonycorn/airflow/output/*'
    )


    clear_output_folder >> write_to_file(groupby_industry(read_and_clean_data()))

dag = taskflow_api_dag()





--------------------------------------------------------------------------------
# Let's set up a more complex pipeline with branches

# data_processing_pipeline_taskflow_v3.py
import os
import pandas as pd
import random
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    'owner' : 'loonycorn',
}

INPUT_FILE = '/Users/loonycorn/airflow/datasets/Firmographic.csv'
OUTPUT_FOLDER = '/Users/loonycorn/airflow/output/'

def read_and_clean_data(**kwargs):
    df = pd.read_csv(INPUT_FILE)
    df = df.dropna()

    industries = df['Industry'].unique().tolist()

    Variable.set("industries", json.dumps(industries))

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    return cleaned_data_json

def choose_industry(**kwargs):
    industries = json.loads(Variable.get('industries'))

    selected_industry = random.choice(industries)

    Variable.set("selected_industry", selected_industry)
    
    return selected_industry

def determine_branch(**kwargs):
    selected_industry = kwargs['ti'].xcom_pull(task_ids='choose_industry')

    if selected_industry == 'Technology':
        return 'filter_technology'
    elif selected_industry == 'Consulting':
        return 'filter_consulting'
    elif selected_industry == 'Manufacturing':
        return 'filter_manufacturing'
    elif selected_industry == 'Engineering':
        return 'filter_engineering'

def filter_industry(**kwargs):
    ti = kwargs['ti']

    selected_industry = Variable.get('selected_industry')
    
    cleaned_data = ti.xcom_pull(task_ids='read_and_clean_data')
    cleaned_data = json.loads(cleaned_data)
    filtered_data = [row for row in cleaned_data if row['Industry'] == selected_industry]
    
    return filtered_data

def write_to_file(**kwargs):
    selected_industry = Variable.get('selected_industry')

    ti = kwargs['ti']
    filtered_data = ti.xcom_pull(task_ids=f'filter_{selected_industry.lower()}')

    if not filtered_data:
        filtered_data = []

    df = pd.DataFrame(filtered_data)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_file = os.path.join(OUTPUT_FOLDER, f"{selected_industry.lower()}_data.csv")
    
    df.to_csv(output_file, index=False)



with DAG(
    dag_id='taskflow_pipeline',
    description = 'Showcases the use of the Taskflow API',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:
    clear_output_folder = BashOperator(
        task_id='clear_output_folder',
        bash_command='rm -f /Users/loonycorn/airflow/output/*'
    )

    read_and_clean_data = PythonOperator(
        task_id='read_and_clean_data',
        python_callable=read_and_clean_data
    )

    choose_industry = PythonOperator(
        task_id='choose_industry',
        python_callable=choose_industry
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )

    filter_technology = PythonOperator(
        task_id='filter_technology',
        op_kwargs={'industry': 'Technology'},
        python_callable=filter_industry,
        do_xcom_push=True
    )

    filter_consulting = PythonOperator(
        task_id='filter_consulting',
        op_kwargs={'industry': 'Consulting'},
        python_callable=filter_industry,
        do_xcom_push=True
    )

    filter_manufacturing = PythonOperator(
        task_id='filter_manufacturing',
        op_kwargs={'industry': 'Manufacturing'},
        python_callable=filter_industry,
        do_xcom_push=True
    )

    filter_engineering = PythonOperator(
        task_id='filter_engineering',
        op_kwargs={'industry': 'Engineering'},
        python_callable=filter_industry,
        do_xcom_push=True
    )

    write_to_file = PythonOperator(
        task_id='write_to_file',
        python_callable=write_to_file,
        trigger_rule='one_success'
    )



clear_output_folder >> read_and_clean_data >> choose_industry >> determine_branch

determine_branch >> filter_technology >> write_to_file
determine_branch >> filter_consulting >> write_to_file
determine_branch >> filter_manufacturing >> write_to_file
determine_branch >> filter_engineering >> write_to_file


# Go to the airflow UI

# Show the Grid view

# Show the Graph view

# Run the DAG and toggle on the Auto-refresh

# Once the run is complete

# Click on Admin -> Variables
# (Observe we have list of variables)

# Show the output folder and the files created

# Run the DAG 2-3 times


--------------------------------------------------------------------------------

# Delete all the files in the output folder

# Delete all the Variables


# IMPORTANT: While recording please add the import first

from airflow.decorators import task, dag

# And then add each @task function one at a time

import os
import pandas as pd
import random
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    'owner' : 'loonycorn',
}

INPUT_FILE = '/Users/loonycorn/airflow/datasets/Firmographic.csv'
OUTPUT_FOLDER = '/Users/loonycorn/airflow/output/'

@task(task_id="read_and_clean", retries=2)
def read_and_clean_data():
    df = pd.read_csv(INPUT_FILE)
    df = df.dropna()

    industries = df['Industry'].unique().tolist()

    Variable.set("industries", json.dumps(industries))

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    return cleaned_data_json

@task(task_id="choose", retries=2)
def choose_industry():
    industries = json.loads(Variable.get('industries'))

    selected_industry = random.choice(industries)

    Variable.set("selected_industry", selected_industry)
    
    return selected_industry

@task.branch(task_id="branch", retries=2)
def determine_branch(**kwargs):
    selected_industry = kwargs['ti'].xcom_pull(task_ids='choose')

    if selected_industry == 'Technology':
        return 'filter_technology'
    elif selected_industry == 'Consulting':
        return 'filter_consulting'
    elif selected_industry == 'Manufacturing':
        return 'filter_manufacturing'
    elif selected_industry == 'Engineering':
        return 'filter_engineering'

def filter_industry(cleaned_data_json: str):
    selected_industry = Variable.get('selected_industry')
    
    cleaned_data = json.loads(cleaned_data_json)
    filtered_data = [row for row in cleaned_data if row['Industry'] == selected_industry]
    
    return filtered_data

@task(task_id="filter_technology", retries=2)
def filter_technology(cleaned_data_json: str):
    return filter_industry(cleaned_data_json)

@task(task_id="filter_consulting", retries=2)
def filter_consulting(cleaned_data_json: str):
    return filter_industry(cleaned_data_json)

@task(task_id="filter_manufacturing", retries=2)
def filter_manufacturing(cleaned_data_json: str):
    return filter_industry(cleaned_data_json)

@task(task_id="filter_engineering", retries=2)
def filter_engineering(cleaned_data_json: str):
    return filter_industry(cleaned_data_json)


@task(task_id="write", retries=2)
def write_to_file(filtered_data: list):
    selected_industry = Variable.get('selected_industry')

    if not filtered_data:
        filtered_data = []

    df = pd.DataFrame(filtered_data)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_file = os.path.join(OUTPUT_FOLDER, f"{selected_industry.lower()}_data.csv")
    
    df.to_csv(output_file, index=False)


@dag(dag_id='taskflow_pipeline',
     description = 'Showcases the use of the Taskflow API',
     default_args = default_args,
     start_date = days_ago(1),
     schedule_interval = '@once')
def taskflow_api_dag():
    clear_output_folder = BashOperator(
        task_id='clear_output_folder',
        bash_command='rm -f /Users/loonycorn/airflow/output/*'
    )

    clean_data = read_and_clean_data()
    selected_industry = choose_industry()

    clear_output_folder >> clean_data >> selected_industry

    branch_op = determine_branch()

    selected_industry >> branch_op

    branch_op >> [
        write_to_file(filter_technology(clean_data)),
        write_to_file(filter_consulting(clean_data)),
        write_to_file(filter_manufacturing(clean_data)),
        write_to_file(filter_engineering(clean_data))
    ] 

dag = taskflow_api_dag()

# Go to the airflow UI

# Show the Grid view

# Show the Graph view

# Run the DAG and toggle on the Auto-refresh

# Once the run is complete

# Click on Admin -> Variables

# Show the output folder and the files created

# Run the DAG 2-3 times

--------------------------------------------------------------------------------



