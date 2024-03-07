# Importing libraries
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook

# Default settings for all the dags in the pipeline
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 23),
    'depends_on_past': False,
    'max_active_runs_per_dag': 1,
    'retries': 0,
    'retries_delay': timedelta(minutes=5)
}

# Extract database target
def extract_db():
    # Assuming you have a MySQL connection ID configured in Airflow called 'source_mysql_conn'
    source_mysql_conn_id = 'source_mysql_conn'
    source_mysql_hook = MySqlHook(mysql_conn_id=source_mysql_conn_id)

    # Replace 'your_database_name' and 'test_user' with your actual database and table names
    query = "SELECT * FROM your_database_name.test_user"
    results = source_mysql_hook.get_pandas_df(sql=query)

    return results

# Transform the data if needed
def transform_data(data):
    # You can perform any transformations on the data here if needed
    # For simplicity, let's assume no transformation is needed in this case
    return data

# Load into the destination database
def load_db(**kwargs):
    target_mysql_conn_id = 'target_mysql_conn'
    target_mysql_hook = MySqlHook(mysql_conn_id=target_mysql_conn_id)

    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_data')

    # Replace 'your_database_name' and 'user_db' with your actual database and table names
    target_mysql_hook.insert_rows(table='your_database_name.user_db', rows=extracted_data.to_dict(orient='records'))

# DAG definition
dag = DAG(
    'db_to_db_dag',
    default_args=default_args,
    description='ETL DAG to move data from MySQL table to another Database',
    schedule_interval='@daily',
    catchup=False
)

# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_db,
    provide_context=True,  # Make sure to include this for passing context
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    op_args=['{{ ti.xcom_pull(task_ids=\'extract_data\') }}'],  # Pass the extracted data as an argument
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_db,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> extract_task >> transform_task >> load_task >> end_task