# For function jsonToCsv
import pandas as pd

#For Apache Airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2023, 6, 1),
    'max_active_runs_per_dag' : 1,
    'concurrency': 1,
    'retries': 0
}

# A JSON string reader to .csv writer function.
def jsonToCsv(url, outputcsv):

    # Reads the JSON string into a pandas DataFrame object.
    data = pd.read_json(url)

    # Convert the object to a .csv file.
    # It is unnecessary to separate the JSON reading and the .csv writing.
    data.to_csv(outputcsv)

    return 'Read JSON and written to .csv'

def csvToSql():

    hook = MySqlHook(mysql_conn_id = 'mysql')

    data_file = '/appdata/sfgov.csv'
    table_name = 'sfgov'

    load_query = f"LOAD DATA INFILE '{data_file}' IGNORE INTO TABLE {table_name} " \
             "FIELDS TERMINATED BY ',' " \
             "LINES TERMINATED BY '\n' " \
             "IGNORE 1 LINES"
    
    hook.run(load_query)

    # # Attempt connection to a database
    # try:
    #     dbconnect = MySQLdb.connect(
    #             host='mysql',
    #             user='airflow',
    #             passwd='airflow',
    #             db='airflow'
    #             )
    # except MySQLdb.Error as e:
    #     print('Can\'t connect.\nError:', e)

    # # Define a cursor iterator object to function and to traverse the database.
    # cursor = dbconnect.cursor()
    # # Open and read from the .csv file
    # with open('/appdata/sfgov.csv') as csv_file:

    #     # Assign the .csv data that will be iterated by the cursor.
    #     csv_data = csv.reader(csv_file)

    #     # Iterate through each row in the CSV file and insert it into MySQL
    #     for row in csv_data:
    #         sql = """INSERT INTO sfgov(number, docusignid, publicurl, filingtype,
    #             cityagencyname, cityagencycontactname, cityagencycontacttelephone,
    #             cityagencycontactemail, bidrfpnumber, natureofcontract, datesigned,
    #             comments, filenumber, originalfilingdate, amendmentdescription,
    #             additionalnamesrequired, signername, signertitle) 
    #             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    #         # Execute the INSERT query
    #         cursor.execute(sql, tuple(row))

    #         # Commit the changes
    #         dbconnect.commit()

    # '''
    # # Print all rows - FOR DEBUGGING ONLY
    # cursor.execute("SELECT * FROM sfgov")
    # rows = cursor.fetchall()

    # print(cursor.rowcount)
    # for row in rows:
    #     print(row)
    # '''

    # # Close the connection
    # cursor.close()

    # Confirm completion
    return 'Read .csv and written to the MySQL database'


# Define the DAG
with DAG(
    dag_id='automate_data_ingestion',
    default_args=default_args,
    catchup=False,
    schedule_interval='@daily'
) as dag:

    extract_json_to_csv = PythonOperator(
            task_id='extract_json_to_csv',
            python_callable=jsonToCsv,
            op_kwargs={
                'url':'https://data.sfgov.org/resource/vw6y-z8j6.json',
                'outputcsv':'/appdata/sfgov.csv'
                }
            )
    
    create_table = MySqlOperator(
            task_id='create_table',
            mysql_conn_id='mysql',
            sql = '''
                    DROP TABLE IF EXISTS sfgov;

                    CREATE TABLE sfgov (
                        number VARCHAR(50),
                        docusignid VARCHAR(100),
                        publicurl VARCHAR(100),
                        filingtype VARCHAR(50),
                        cityagencyname VARCHAR(100),
                        cityagencycontactname VARCHAR(100),
                        cityagencycontacttelephone VARCHAR(50),
                        cityagencycontactemail VARCHAR(50),
                        bidrfpnumber VARCHAR(100),
                        natureofcontract VARCHAR(255),
                        datesigned VARCHAR(50),
                        comments VARCHAR(255),
                        filenumber VARCHAR(50),
                        originalfilingdate VARCHAR(100),
                        amendmentdescription VARCHAR(255),
                        additionalnamesrequired VARCHAR(50),
                        signername VARCHAR(100),
                        signertitle VARCHAR(100)
                    );
                '''
    )

    opr_csv_to_sql = PythonOperator(
            task_id='load_csv_to_sql',
            python_callable=csvToSql
            )
    
# The actual workflow
extract_json_to_csv >> create_table >> opr_csv_to_sql

