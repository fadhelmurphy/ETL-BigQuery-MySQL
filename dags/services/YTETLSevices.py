from __future__ import annotations

import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from mysql.connector import connect
from mysql.connector import Error
from sqlalchemy import create_engine
from utils import preprocess_text


def s_extract_yt(credentials):
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


def s_transform_yt(data):
    # Mengekstrak data
    items = data.get('items', [])

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
    # Convert count fields to integer
    df['likeCount'] = df['likeCount'].astype(int)
    df['viewCount'] = df['viewCount'].astype(int)
    df['commentCount'] = df['commentCount'].astype(int)
    # df.to_csv(tmp_file_path, index=False) # debug only
    return df


def s_load_gbq(df, client, DATASET_ID, TABLE_ID):
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
            bigquery.SchemaField('likeCount', 'INTEGER'),
            bigquery.SchemaField('viewCount', 'INTEGER'),
            bigquery.SchemaField('commentCount', 'INTEGER'),
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


def s_load_mysql(df, MYSQL_ROOT_USER, MYSQL_ROOT_PASSWORD, database, table):
    ENGINE_CONNECT = f'mysql+mysqlconnector://{MYSQL_ROOT_USER}\
        :{MYSQL_ROOT_PASSWORD}@mysql:3306/{database}'

    MYSQL_HOST = 'mysql'

    try:
        # Connect to MySQL server
        with connect(
            host=MYSQL_HOST,
            user=MYSQL_ROOT_USER,
            password=MYSQL_ROOT_PASSWORD,
        ) as connection:
            # Create a cursor object
            cursor = connection.cursor()

            # Create the database if it does not exist
            cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')

            # Commit the changes
            connection.commit()

    except Error as e:
        print(f'Error creating database: {e}')
        return

    # Now connect to the specific database 'etl_db'
    engine = create_engine(ENGINE_CONNECT)

    return df.to_sql(table, con=engine, if_exists='append')
