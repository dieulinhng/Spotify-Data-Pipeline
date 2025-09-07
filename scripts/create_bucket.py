import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud.json'

storage_client = storage.Client()

dir(storage_client)

bucket_names = [
    'artist_names_batch',
    'spotify_kworb_artist_names',
    'artist_id_and_name',
    'spotify_raw_bucket',
    'spotify_transform_bucket',
    'spotify_raw_bucket_v2',
    'spotify_transform_bucket_v2'
]

for name in bucket_names:
    try:
        storage_client.get_bucket(name)
    except Exception as e:
        bucket = storage_client.bucket(name)
        bucket.location = 'US'
        storage_client.create_bucket(bucket)