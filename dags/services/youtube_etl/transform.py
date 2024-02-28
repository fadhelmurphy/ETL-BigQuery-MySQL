from __future__ import annotations

import sys

import pandas as pd
from utils import preprocess_text
sys.path.append('...')


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
