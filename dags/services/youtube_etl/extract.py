from __future__ import annotations

from googleapiclient.discovery import build


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
