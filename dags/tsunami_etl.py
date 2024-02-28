from datetime import datetime, timedelta
from airflow import DAG
# from airflow.providers.google.transfers.bigquery_to_mysql import BigQueryToMySqlOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.mysql_operator import MySqlOperator
# import pandas as pd
from sqlalchemy import create_engine
import pandas_gbq

import os
from dotenv import load_dotenv
from google.oauth2 import service_account
import pandas as pd
from mysql.connector import connect, Error


# Mendapatkan path ke direktori dags
dags_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(dags_dir, "auth/credentials.json")
credentials = service_account.Credentials.from_service_account_file(credentials_path)
tmp_filename = "tmp_file.csv"
tmp_file_path = f'/opt/airflow/dags/{tmp_filename}'

load_dotenv()

PROJECT_ID = os.getenv('GCP_PROJECT_ID')

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# } # on debug

dag = DAG(
    'tsunami_etl',
    schedule_interval=None, # on debug
    start_date=datetime(2024, 1, 1), # on debug
    # default_args=default_args, # on debug
    description='ETL pipeline for Tsunami data',
    # schedule_interval='@hourly',  # Set the schedule to run every hour # on debug
)

def extract_tsunami():
    # Perform the BigQuery extraction here
    # Replace with your actual BigQuery SQL query and dataset
    query = """
        SELECT COUNT(DISTINCT(id)) AS record, location_name FROM `bigquery-public-data.noaa_tsunami.historical_runups` GROUP BY location_name ORDER BY record DESC
    """

    return pandas_gbq.read_gbq(query, project_id=PROJECT_ID, dialect='standard', 
                               credentials=credentials
                                )

def transform_tsunami(**kwargs):
    # Perform any necessary data transformations
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_task')
    return df

def load_to_mysql(**kwargs):
    # Load the transformed data into MySQL
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_task')

    print(os.environ)

    ENGINE_CONNECT = os.getenv('ENGINE_CONNECT')
    
    MYSQL_ROOT_USER = os.getenv('AIRFLOW_MYSQL_USER')
    MYSQL_ROOT_PASSWORD = os.getenv('AIRFLOW_MYSQL_PASSWORD')
    MYSQL_HOST = 'mysql'
    MYSQL_DATABASE = 'etl_db'

    try:
        # Connect to MySQL server
        with connect(
            host=MYSQL_HOST,
            user=MYSQL_ROOT_USER,
            password=MYSQL_ROOT_PASSWORD
        ) as connection:
            # Create a cursor object
            cursor = connection.cursor()

            # Create the database if it does not exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")

            # Commit the changes
            connection.commit()

    except Error as e:
        print(f"Error creating database: {e}")
        return

    # Now connect to the specific database 'etl_db'
    engine = create_engine(ENGINE_CONNECT)
    
    df.to_sql('tsunami', con=engine, index=False, if_exists='replace')

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_tsunami,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_tsunami,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_to_mysql,
    provide_context=True,
    dag=dag,
)

if __name__ == "__main__":
    extract_task >> transform_task >> load_task
