import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator

from datetime import datetime, timedelta
import pandas as pd
# from pendulum import date
# from bs4 import BeautifulSoup
# import requests
import json
from pandas import json_normalize


# Grab current date
current_date = datetime.today().strftime('%Y-%m-%d')

# Default settings for all the dags in the pipeline
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 20),
    'depends_on_past': False,
    'max_active_runs_per_dag' : 1,
    'retries': 0,
    'retries_delay': timedelta(minutes=5)
}

#Function transform data source menjadi dataframe
def _process_data(ti):

    data = ti.xcom_pull(task_ids = 'extract_data') # Extract the json object from XCom
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
    
    # Export DataFrame to CSV (File located in worker container)
    file_name = f'/appdata/processed_data_{current_date}.csv'

    processed_data.to_csv(file_name, index=None, header=False)

#Function load ke database MySQL
def _store_data():
    '''
    This function uses the MySQL Hook to store data from processed_data.csv
    and into the table
    
    '''
    # Connect to the MySql connection
    hook = MySqlHook(mysql_conn_id = 'mysql')

    data_file = f'/appdata/processed_data_{current_date}.csv'
    # data_file = '/appdata/processed_data.csv'
    table_name = 'rates'

    load_query = f"LOAD DATA INFILE '{data_file}' INTO TABLE {table_name} " \
             "FIELDS TERMINATED BY ',' " \
             "LINES TERMINATED BY '\n' " \
             "IGNORE 1 ROWS"
    
    hook.run(load_query)

with DAG(
    dag_id='forex_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    #DAG 1 [Check if API available]
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='is_api_available', #write this while create conn id
        method='GET',
        endpoint= current_date + '?access_key=c59dba13783dea198012827450527b56',
        response_check= lambda response: 'EUR' in response.text
    )

    #DAG 2 [Create a table]
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='mysql',
        sql='''

            drop table if exists rates;

            create table rates(
                rate float not null,
                symbol text not null
            );
        
        '''
    )

    #DAG 3 [Extract data]
    extract_data_task = SimpleHttpOperator(
        task_id='extract_data',
        http_conn_id='is_api_available',
        method='GET',
        endpoint= current_date + '?access_key=c59dba13783dea198012827450527b56',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    #DAG 4 [Transform data]
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=_process_data,
    )

    #DAG 5 [Load data]
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=_store_data,
    )

#Dependencies
is_api_available >> create_table >> extract_data_task >> transform_data_task >> load_data_task