from __future__ import annotations

from google.cloud import bigquery


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
