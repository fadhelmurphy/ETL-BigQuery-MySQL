from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from utils import preprocess_text

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


def extract_tsunami():
    youtube = build(
        'youtube', 'v3',
        credentials=credentials,
    )

    # Make API request for videos
    request = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        chart='mostPopular',
        regionCode='id',
        maxResults=10,
    )
    response = request.execute()

    return response


def transform_yt(**kwargs):
    # Perform any necessary data transformations
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='extract_task')
    # Mengekstrak data
    items = response.get('items', [])

    data_list = []
    for item in items:
        snippet = item.get('snippet', {})
        statistics = item.get('statistics', {})
        thumbnails = snippet.get('thumbnails', {})

        data = {
            'channelTitle': snippet.get('channelTitle', ''),
            'title': snippet.get('title', ''),
            'description': snippet.get('description', ''),
            'default_thumbnail': thumbnails.get('default', {}).get('url', ''),
            'tags': ','.join(snippet.get('tags', [])),
            'likeCount': statistics.get('likeCount', ''),
            'viewCount': statistics.get('viewCount', ''),
            'commentCount': statistics.get('commentCount', ''),
        }

        data_list.append(data)

    # Membuat DataFrame
    df = pd.DataFrame(data_list)
    # Apply the preprocessing function to the 'title' and 'description' columns
    df['title'] = df['title'].apply(preprocess_text)
    df['description'] = df['description'].apply(preprocess_text)
    # df.to_csv(tmp_file_path, index=False) # debug only
    return df


def load_gbq(**kwargs):

    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_task')
    DATASET_ID = os.getenv('AIRFLOW_DATASET_ID')
    TABLE_ID = os.getenv('AIRFLOW_TABLE_ID')

    # Mengecek apakah tabel sudah ada
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    if not client.get_table(table_ref):
        # Jika tabel belum ada, membuatnya
        schema = [
            bigquery.SchemaField('channelTitle', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('description', 'STRING'),
            bigquery.SchemaField('default_thumbnail', 'STRING'),
            bigquery.SchemaField('tags', 'STRING'),
            bigquery.SchemaField('likeCount', 'STRING'),
            bigquery.SchemaField('viewCount', 'STRING'),
            bigquery.SchemaField('commentCount', 'STRING'),
        ]

        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        # Memuat DataFrame ke tabel dengan menggunakan schema yang telah dibuat
        # Ganti 'WRITE_TRUNCATE' dengan 'WRITE_APPEND'
        # jika ingin menambahkan data
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition='WRITE_TRUNCATE',
        )
        client.load_table_from_dataframe(
            df, table_ref, job_config=job_config,
        ).result()
    else:
        # Jika tabel sudah ada, memuat DataFrame
        # ke tabel tanpa menyertakan schema
        client.load_table_from_dataframe(
            df,
            table_ref,
        ).result()


extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_tsunami,
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
