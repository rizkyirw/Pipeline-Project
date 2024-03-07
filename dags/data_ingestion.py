from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
from pandas import json_normalize
import requests
import json

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2023, 6, 1),
    'max_active_runs_per_dag' : 1,
    'retries': 1,
    'retries_delay': timedelta(minutes=5)
}

# Function to fetch data from the API
def fetch_data_from_api():
    response = requests.get('http://data.fixer.io/api/latest?access_key=8309a8ceda167833b06d0b00ccf6d36e')
    data = response.json()
    return data

# Function to process and transform data
def process_data(ti):
    data = ti.xcom_pull(task_ids = 'fetch_data') # Extract the json object from XCom
    data = data['rates'] # Extract the symbols and the rates in a dictionary 
    processed_data = pd.DataFrame(columns=['rate', 'symbol']) # Create an empty DataFrame
    temp_df = dict() # Create an empty dictionary

    # Iterate through the symbols, create a temp dataframe from each symbol and append it to the DataFrame
    for symbol,rate in data.items():
    
        temp_df = json_normalize({
            
            'rate': float(rate),
            'symbol' :symbol
            
        })
    
        processed_data = processed_data.append(temp_df)
    return processed_data

# Function to load data into MySQL

def load_data_to_mysql(ti):
    # Load data from XCom
    data = ti.xcom_pull(task_ids='process_data', key='processed_data')
    
    # Check if data has 'rates' key
    if 'rates' not in data:
        raise KeyError("'rates' key not found in data")
        
    # Get rates from data
    rates = data['rates']
    
    # Connect to MySQL
    mysql_conn_id = 'mysql'
    mysql_hook = MySqlHook(mysql_conn_id)
    conn = mysql_hook.get_conn()
    
    # Insert data into MySQL
    cursor = conn.cursor()
    try:
        for entry in rates:
            query = "INSERT INTO rates (rate, symbol) VALUES (%s, %s)"
            cursor.execute(query, (entry['rate'], entry['symbol']))
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


# Define the DAG
with DAG(
    dag_id='data_ingestion',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:

    # Define the tasks
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api,
        dag=dag
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        dag=dag
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_mysql',
        python_callable=load_data_to_mysql,
        dag=dag
    )

# Set the task dependencies
fetch_data_task >> process_data_task >> load_data_task
