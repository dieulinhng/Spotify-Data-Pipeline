import os
from google.cloud import storage
from pathlib import Path

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud.json'

storage_client = storage.Client()

local_path = 'data/artist_names_kworb/artist_names.txt'
bucket_name = "spotify_kworb_artist_names"
blob_path = 'artist_names.txt'

bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(blob_path)
blob.upload_from_filename(local_path, content_type='text/plain; charset=utf-8')