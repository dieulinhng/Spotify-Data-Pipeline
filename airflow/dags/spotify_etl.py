import os
import json
import time
import pandas as pd 
import pyarrow
import random
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datetime import datetime, timedelta

import spotipy
from spotipy import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import PreconditionFailed

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.models import Variable

import io
from io import BytesIO

from dotenv import load_dotenv 

load_dotenv() 

retries = Retry(
    total=5,
    allowed_methods=frozenset({'GET', 'POST'}),
    status_forcelist=[429, 500, 502, 503, 504],
    backoff_factor=1,
    raise_on_status=False,
    respect_retry_after_header=True
)

adapter = HTTPAdapter(max_retries=retries)

session = requests.Session()
session.mount('https://', adapter)
session.mount('http://', adapter)

SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID') 
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET') 
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, requests_session=session), 
                     requests_session=session,
                     requests_timeout=30)

# Spotify's request rate limit
def limit(sp_list, size):
    len_list = []
    for item in sp_list:
        len_list.append(item)
        if len(len_list) == size:
            yield len_list
            len_list = []
    if len_list:
        yield len_list

# Get the artist_id from artist_id_and_name
def read_artist_id_from_gcs(id_bucket):
    hook = GCSHook(gcp_conn_id='gcp_spotify')
    client = hook.get_conn()
    blobs = sorted(client.list_blobs(id_bucket), key=lambda b: b.name)
    
    ids = set()
    
    for b in blobs:
        name = b.name
        content = hook.download(bucket_name=id_bucket, object_name=name)
        if isinstance(content, (bytes, bytearray)):
            c = content
        else:
            c = content.read()
        data = json.loads(c.decode('utf-8'))
        for row in data:
            artist_id = row.get('artist_id')
            if artist_id:
                ids.add(artist_id)
    
    return list(ids)

def daily_ids(id_bucket, daily=30, **kwargs):
    ids = read_artist_id_from_gcs(id_bucket)
    run_date = kwargs['ds_nodash']
    pick_id = random.Random(int(run_date))
    if len(ids) <= daily:
        return ids
    return pick_id.sample(ids, daily)

# Extract artists, albums and tracks
def extract_artists(**kwargs):
    hook = GCSHook(gcp_conn_id = 'gcp_spotify')
    artist_ids = daily_ids('artist_id_and_name', daily=30, **kwargs)
    spotify_raw_bucket = 'spotify_raw_bucket'
    run_date = kwargs['ds_nodash']
    
    artists = []

    for art in limit(artist_ids, 50):
        try:
            spotify_artists = sp.artists(art)
        except spotipy.SpotifyException as e:
            logging.exception('Failed')
            if getattr(e, "http_status", None) == 401 or "access token expired" in str(e).lower():
                spotify_artists = sp.artists(art)
            else:
                time.sleep(0.7)
                continue
        for a in spotify_artists.get('artists', []):
            artists.append({
                'artist_id': a.get('id'),
                'artist_name': a.get('name'),
                'genres': a.get('genres') or [],
                'followers': (a.get('followers') or {}).get('total'),
                'popularity': a.get('popularity')
            })
        time.sleep(0.5)

    json_artist = json.dumps(artists, indent=2, ensure_ascii=False).encode('utf-8')

    hook.upload(bucket_name = spotify_raw_bucket,
                object_name = f'{run_date}/artist.json',
                data = json_artist,
                mime_type = 'application/json')    

# Get albums
def extract_albums(**kwargs):
    hook = GCSHook(gcp_conn_id = 'gcp_spotify')
    artist_ids = daily_ids('artist_id_and_name', daily=30, **kwargs)
    spotify_raw_bucket = 'spotify_raw_bucket'
    run_date = kwargs['ds_nodash']
    
    albums = []
    
    for artist_id in artist_ids:
        offset = 0
        while True:
            try:
                spotify_albums = sp.artist_albums(artist_id, include_groups='album,single,compilation,appears_on', limit=25, offset=offset)
            except spotipy.SpotifyException as e:
                logging.exception('Failed')
                if getattr(e, "http_status", None) == 401 or "access token expired" in str(e).lower():
                    spotify_albums = sp.artist_albums(artist_id, include_groups='album,single,compilation,appears_on', limit=25, offset=offset)
                else:
                    time.sleep(0.7)
                    break
            for alb in spotify_albums.get('items', []):
                for artist in alb.get('artists', []):
                    albums.append({
                        'artist_id': artist.get('id'),
                        'artist_name': artist.get('name'),
                        'album_id': alb.get('id'),
                        'album_name': alb.get('name'),
                        'release_date': alb.get('release_date'),
                        'type': alb.get('type'),
                        'total_tracks': alb.get('total_tracks'),
                        'album_group': alb.get('album_group'),
                    })
            offset += len(spotify_albums.get('items', []))
            if not spotify_albums.get('next'):
                break
            time.sleep(0.5)
        time.sleep(0.5)
    
    json_album = json.dumps(albums, indent=2, ensure_ascii=False).encode('utf-8')
    hook.upload(bucket_name = spotify_raw_bucket,
                object_name = f'{run_date}/album.json',
                data = json_album,
                mime_type = 'application/json')
# Get tracks 
def extract_tracks(**kwargs): 
    hook = GCSHook(gcp_conn_id = 'gcp_spotify')
    spotify_raw_bucket = 'spotify_raw_bucket'
    run_date = kwargs['ds_nodash']
    
    obj = hook.download(bucket_name=spotify_raw_bucket, object_name=f'{run_date}/album.json')
    if isinstance(obj, (bytes, bytearray)):
        o = obj
    else:
        o = obj.read()
    albums = json.loads(o.decode('utf-8'))
    
    tracks = []
    
    for alb in albums:
        album_id = alb['album_id']
        offset = 0
        while True:
            try:
                spotify_tracks = sp.album_tracks(album_id, limit=50, offset=offset)
            except spotipy.SpotifyException as e:
                logging.exception('Failed')
                if getattr(e, "http_status", None) == 401 or "access token expired" in str(e).lower():
                    spotify_tracks = sp.album_tracks(album_id, limit=50, offset=offset)
                else:
                    time.sleep(0.5)
                    break
            for tr in spotify_tracks.get('items', []):
                for artist in tr.get('artists', []):
                    tracks.append({
                        'artist_id': artist.get('id'),
                        'artist_name': artist.get('name'),
                        'track_id': tr.get('id'),
                        'track_name': tr.get('name'),
                        'track_number': tr.get('track_number'),
                        'duration_ms': tr.get('duration_ms'),
                        'explicit': tr.get('explicit'),
                    })
            offset += len(spotify_tracks.get('items', []))
            if not spotify_tracks.get('next'):
                break
            time.sleep(0.5)
        time.sleep(0.5)
        
    json_track = json.dumps(tracks, indent=2, ensure_ascii=False).encode('utf-8')
  
    hook.upload(bucket_name = spotify_raw_bucket,
                object_name = f'{run_date}/track.json',
                data = json_track,
                mime_type = 'application/json') 
    
# Transform data
def transform(**kwargs): 
    hook = GCSHook(gcp_conn_id = 'gcp_spotify')
    spotify_raw_bucket = 'spotify_raw_bucket'
    spotify_transform_bucket = 'spotify_transform_bucket'
    
    run_date = kwargs['ds_nodash']
    
    obj = hook.download(bucket_name=spotify_raw_bucket, object_name=f'{run_date}/artist.json')
    if isinstance(obj, (bytes, bytearray)):
        o = obj
    else:
        o = obj.read()
    artist = json.loads(o.decode('utf-8'))
    
    obj = hook.download(bucket_name=spotify_raw_bucket, object_name=f'{run_date}/album.json')
    if isinstance(obj, (bytes, bytearray)):
        o = obj
    else:
        o = obj.read()
    album = json.loads(o.decode('utf-8'))
    
    obj = hook.download(bucket_name=spotify_raw_bucket, object_name=f'{run_date}/track.json')
    if isinstance(obj, (bytes, bytearray)):
        o = obj
    else:
        o = obj.read()
    track = json.loads(o.decode('utf-8'))
    
    df_artist = pd.DataFrame(artist, columns=['artist_id', 'artist_name', 'genres', 'followers', 'popularity'])
    df_album  = pd.DataFrame(album, columns=['artist_id', 'artist_name', 'album_id', 'album_name', 'release_date', 'type', 'total_tracks', 'album_group'])
    df_track  = pd.DataFrame(track, columns=['artist_id', 'artist_name', 'track_id', 'track_name', 'track_number', 'duration_ms', 'explicit'])
    
    artist_buffer = io.BytesIO()
    df_artist.to_parquet(artist_buffer, index=False)
    artist_buffer.seek(0)
    hook.upload(
        bucket_name=spotify_transform_bucket,
        object_name=f'{run_date}/artist.parquet',
        data=artist_buffer.read()
    )

    album_buffer = io.BytesIO()
    df_album.to_parquet(album_buffer, index=False)
    album_buffer.seek(0)
    hook.upload(
        bucket_name=spotify_transform_bucket,
        object_name=f'{run_date}/album.parquet',
        data=album_buffer.read()
    )

    track_buffer = io.BytesIO()
    df_track.to_parquet(track_buffer, index=False)
    track_buffer.seek(0)
    hook.upload(
        bucket_name=spotify_transform_bucket,
        object_name=f'{run_date}/track.parquet',
        data=track_buffer.read()
    )

# Load into BigQuery
def load(**kwargs):
    BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
    BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
    
    spotify_transform_bucket = 'spotify_transform_bucket'
    run_date = kwargs['ds_nodash']
    hook = GoogleBaseHook(gcp_conn_id='gcp_spotify')
    credentials = hook.get_credentials()
    bigquery_client = bigquery.Client(project=BQ_PROJECT_ID, credentials=credentials)
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    
    entity = ['artist', 'album', 'track']
    for name in entity:
        uri = f'gs://{spotify_transform_bucket}/{run_date}/{name}.parquet'
        table_id = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{name}'
        load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()

# DAG
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'max_retry_delay': timedelta(minutes=30),
    'retry_exponential_backoff': True
}

with DAG(
    dag_id = 'spotify_etl_v04',
    default_args = default_args,
    description = 'ETL from Spotify Web API to BigQuery',
    schedule = '@daily',
    start_date = datetime(2025, 8, 29),
    max_active_runs = 1
) as dag:
    
    extract_artists_task = PythonOperator(
        task_id = 'extract_artists',
        python_callable = extract_artists
    )
    extract_albums_task = PythonOperator(
        task_id = 'extract_albums',
        python_callable = extract_albums
    )
    extract_tracks_task = PythonOperator(
        task_id = 'extract_tracks',
        python_callable = extract_tracks
    )
    transform_task = PythonOperator(
        task_id = 'transform',
        python_callable = transform
    )
    load_task = PythonOperator(
        task_id = 'load',
        python_callable = load
    )

    extract_artists_task >> extract_albums_task >> extract_tracks_task >> transform_task >> load_task