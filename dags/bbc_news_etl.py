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

load_dotenv()

MYSQL_ROOT_USER = os.getenv('MYSQL_ROOT_USER')
MYSQL_ROOT_PASSWORD = os.getenv('MYSQL_ROOT_PASSWORD')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tsunami_etl',
    default_args=default_args,
    description='ETL pipeline for Tsunami data',
    schedule_interval='@hourly',  # Set the schedule to run every hour
)

def extract_tsunami():
    # Perform the BigQuery extraction here
    # Replace with your actual BigQuery SQL query and dataset
    query = """
        SELECT COUNT(DISTINCT(id)) AS record, location_name
        FROM `bigquery-public-data.noaa_tsunami.historical_runups`
        GROUP BY location_name
        ORDER BY record DESC
    """
    return pandas_gbq.read_gbq(query, project_id=PROJECT_ID, dialect='standard')

def transform_tsunami(data):
    # Perform any necessary data transformations
    # For example, select relevant columns
    return data[['title', 'published_at', 'content']]

def load_to_mysql(**kwargs):
    # Load the transformed data into MySQL
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_task')
    
    # Replace connection parameters with your MySQL connection details
    engine = create_engine('mysql://{MYSQL_ROOT_USER}:{MYSQL_ROOT_PASSWORD}@mysql/etl_db')
    
    df.to_sql('bbc_news', con=engine, index=False, if_exists='replace')

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

extract_task >> transform_task >> load_task
