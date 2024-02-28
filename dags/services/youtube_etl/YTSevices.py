from __future__ import annotations

import sys

import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from utils import preprocess_text
sys.path.append('...')


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
