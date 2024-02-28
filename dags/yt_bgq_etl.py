from datetime import datetime
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
from googleapiclient.discovery import build


# Mendapatkan path ke direktori dags
dags_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(dags_dir, "auth/credentials.json")
credentials = service_account.Credentials.from_service_account_file(credentials_path)
tmp_filename = "tmp_file.csv"
tmp_file_path = f'/opt/airflow/dags/{tmp_filename}'

# load_dotenv("dags/.env")

dag = DAG(
    'yt_trend_etl',
    schedule_interval=None, # on debug
    start_date=datetime(2024, 1, 1), # on debug
    description='ETL pipeline for Youtube Trending data',
    # schedule_interval='@daily',  # Set the schedule to run every day # on debug
)

def extract_tsunami():
    youtube = build("youtube", "v3", credentials=credentials)

    # Make API request for videos
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        chart="mostPopular",
        regionCode="id",
        maxResults=10,
    )
    response = request.execute()

    return response

def transform_yt(**kwargs):
    # Perform any necessary data transformations
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='extract_task')
        # Mengekstrak data
    items = response.get("items", [])

    data_list = []
    for item in items:
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        thumbnails = snippet.get("thumbnails", {})

        data = {
            "channelTitle": snippet.get("channelTitle", ""),
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "default_thumbnail": thumbnails.get("default", {}).get("url", ""),
            "tags": ",".join(snippet.get("tags", [])),
            "likeCount": statistics.get("likeCount", ""),
            "viewCount": statistics.get("viewCount", ""),
            "commentCount": statistics.get("commentCount", ""),
        }

        data_list.append(data)

    # Membuat DataFrame
    df = pd.DataFrame(data_list)
    # df.to_csv(tmp_file_path, index=False) # debug only
    return df

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

if __name__ == "__main__":
    extract_task >> transform_task
