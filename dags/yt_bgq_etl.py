from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from services.youtube_etl.extract import s_extract_yt
from services.youtube_etl.load_gbq import s_load_gbq
from services.youtube_etl.transform import s_transform_yt

load_dotenv('dags/.env')

PROJECT_ID = os.getenv('GCP_PROJECT_ID')


# Mendapatkan path ke direktori dags
dags_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(
    dags_dir,
    'auth/credentials.json',
)
credentials = service_account.Credentials.from_service_account_file(
    credentials_path,
)
# Inisialisasi koneksi ke BigQuery
client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID,
)
tmp_filename = 'tmp_file.csv'
tmp_file_path = f'/opt/airflow/dags/{tmp_filename}'

dag = DAG(
    'yt_trend_etl',
    schedule_interval=None,  # on debug
    start_date=datetime(2024, 1, 1),  # on debug
    description='ETL pipeline for Youtube Trending data',
    # schedule_interval='@daily',
    # Set the schedule to run every day # on debug
)


def extract_yt():
    return s_extract_yt(credentials=credentials)


def transform_yt(**kwargs):
    # Perform any necessary data transformations
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='extract_task')
    return s_transform_yt(data=response)


def load_gbq(**kwargs):

    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_task')
    DATASET_ID = os.getenv('AIRFLOW_DATASET_ID')
    TABLE_ID = os.getenv('AIRFLOW_TABLE_ID')
    return s_load_gbq(
        df=df, client=client,
        DATASET_ID=DATASET_ID,
        TABLE_ID=TABLE_ID,
    )


extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_yt,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_yt,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_gbq,
    provide_context=True,
    dag=dag,
)

if __name__ == '__main__':
    extract_task >> transform_task >> load_task
