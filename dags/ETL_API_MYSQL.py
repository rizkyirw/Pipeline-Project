import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

import requests
from datetime import datetime, timedelta
import pandas as pd
import json
from pandas import json_normalize


# Grab current date
current_date = datetime.today().strftime('%Y-%m-%d')

# Default settings for all the dags in the pipeline
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'max_active_runs_per_dag' : 1,
    'retries': 0,
    'retries_delay': timedelta(minutes=5)
}

#Function ekstrak data dari data source
def extract_data():
    # API endpoint
    endpoint = "https://api.restful-api.dev/objects"

    # Airflow task log
    print("Extracting data from the API...")

    # HTTP request to retrieve data
    response = requests.get(endpoint)
    data = response.json()

    # Push extracted data to XCom
    return data

#Function transform data source menjadi dataframe
def _process_data(ti):
    data = ti.xcom_pull(task_ids='extract_data')
    extracted_data = data  # Assuming the extracted data is a list of dictionaries

    processed_data = pd.DataFrame(columns=['item_id', 'item_name', 'color', 'capacity', 'price'])  # Create an empty DataFrame

    for item in extracted_data:
        item_id = item['id']
        item_name = item['name']
        item_data = item.get('data')  # Retrieve 'data' with a default value of None

        color = item_data.get('color') if item_data else None
        capacity = item_data.get('capacity') if item_data else None
        price = item_data.get('price') if item_data else None

        temp_df = pd.DataFrame({
            'item_id': [item_id],
            'item_name': [item_name],
            'color': [color],
            'capacity': [capacity],
            'price': [price]
        })

        processed_data = pd.concat([processed_data, temp_df], ignore_index=True)

    # Export DataFrame to CSV
    file_name = f'/appdata/data_produk_{current_date}.csv'
    processed_data.to_csv(file_name, index=None, header=False)


#Function load ke database MySQL
def _store_data():
    '''
    This function uses the MySQL Hook to store data from processed_data.csv
    and into the table
    
    '''
    # Connect to the MySql connection
    hook = MySqlHook(mysql_conn_id = 'mysql')

    data_file = f'/appdata/data_produk_{current_date}.csv'
    
    table_name = 'inventory_produk'

    load_query = f"LOAD DATA INFILE '{data_file}' IGNORE INTO TABLE {table_name} " \
             "FIELDS TERMINATED BY ',' " \
             "LINES TERMINATED BY '\n' " \
             "IGNORE 1 ROWS"
    
    hook.run(load_query)

with DAG(
    dag_id='Data_Harga_Pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    #DAG 1 [Extract data]
    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
        dag=dag,
    )

    #DAG 2 [Transform data]
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=_process_data,
        provide_context=True,
        dag=dag,
    )

    #DAG 3 [Load data]
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=_store_data,
        provide_context=True,
        dag=dag,
    )

#Dependencies
extract_data_task >> transform_data_task >> load_data_task