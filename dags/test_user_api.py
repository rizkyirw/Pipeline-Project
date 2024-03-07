from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import date, datetime, timedelta
import pandas as pd
import requests
from pandas import json_normalize

# Grab current date
current_date = date.today().strftime('%Y-%m-%d')

# Default settings for all the dags in the pipeline
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 14),
    'depends_on_past': False,
    'max_active_runs_per_dag' : 1,
    'retries': 0,
    'retries_delay': timedelta(minutes=5)
}

# Function to fetch JSON data and store it in XCom
def _fetch_data():
    url = 'https://jsonplaceholder.typicode.com/users/'
    response = requests.get(url)
    data = response.json()
    return data

# Function to transform JSON data into DataFrame
def transform_json_to_df(json_data):
    return json_normalize(json_data)

# Function to process data and store it in XCom
def _process_data(ti):
    data = ti.xcom_pull(task_ids='fetch_data')

    # Transform JSON data to DataFrame
    processed_data = transform_json_to_df(data)

    # Directly store data into XCom
    ti.xcom_push(key='user_data', value=processed_data)

    # Export DataFrame to CSV (File located in worker container)
    file_name = f'/data_user/user_data_{current_date}.csv'
    processed_data.to_csv(file_name, index=None)

# Function to load CSV data into MySQL
def _load_to_mysql():
    hook = MySqlHook(mysql_conn_id='mysql_connection')

    data_file = f'/data_user/user_data_{current_date}.csv'
    table_name = 'test_user'

    load_query = f"LOAD DATA INFILE '{data_file}' INTO TABLE {table_name} " \
                 "FIELDS TERMINATED BY ',' " \
                 "LINES TERMINATED BY '\n' " \
                 "IGNORE 1 ROWS"
    
    hook.run(load_query)

# Define the DAG
with DAG(
    'Test_User_API',
    default_args=default_args,
    description='Testing API',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # Dummy start point
    start_point = DummyOperator(
        task_id='start_task',
        dag=dag,
    )

    # Task to fetch JSON data
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=_fetch_data,
    )

    # Task to process data and store it in XCom
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=_process_data,
        provide_context=True,
    )

    # Task to load CSV data into MySQL
    load_to_mysql_task = PythonOperator(
        task_id='load_to_mysql',
        python_callable=_load_to_mysql,
    )

    # Define the task sequence
    start_point >> fetch_data_task >> process_data_task >> load_to_mysql_task
