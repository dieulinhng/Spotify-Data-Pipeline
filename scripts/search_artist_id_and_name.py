import os
import json
from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from dotenv import load_dotenv

load_dotenv()

def read_input_batch(source_bucket):
    client = storage.Client()
    blob = sorted(client.list_blobs(source_bucket), key=lambda b: b.name)
  
    for b in blob:
        with b.open('r', encoding='utf-8') as f:
            data = json.load(f)
            batch_id = data.get('batch_id')
            artist_names = [' '.join(names.strip().split()) for names in data.get('artist_names', [])]
        yield batch_id, artist_names, b.name

def search_artists_for_all_batches(source_bucket, destination_bucket):
    SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
    SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
    spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET), requests_timeout=10)
    
    client = storage.Client()
    dst_bucket = client.bucket(destination_bucket)
    
    search_artists = []
    
    for batch_id, artist_names, _ in read_input_batch(source_bucket):
        records = []
        
        for n in artist_names:
            escaped_name = n.replace('"', '\\"')
            res = spotify.search(q=f'artist:"{escaped_name}"', type='artist', limit=20)
            items = res.get('artists', {}).get('items', [])
            if not items:
                continue
            for item in items:
                artist_id = item.get('id')
                artist_name = item.get('name')
                records.append({'artist_id': artist_id, 'artist_name': artist_name})
        
        dst_blob_name = f'batch_{batch_id:06d}.json'
        dst_blob = dst_bucket.blob(dst_blob_name)
        json_file = json.dumps(records, ensure_ascii=False, indent=2).encode('utf-8')
        
        try: 
            dst_blob.upload_from_string( 
                json_file, 
                content_type = 'application/json',
                if_generation_match = 0
            ) 
        except PreconditionFailed: 
            continue 
        
        search_artists.append(dst_blob_name) 
    return search_artists

if __name__ == '__main__':
    source_bucket = 'artist_names_batch'   
    destination_bucket = 'artist_id_and_name'   

    created = search_artists_for_all_batches(
        source_bucket=source_bucket,
        destination_bucket=destination_bucket
    )