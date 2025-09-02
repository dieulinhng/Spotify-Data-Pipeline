import json 
from google.cloud import storage 
from google.api_core.exceptions import PreconditionFailed 

def create_batches(source_bucket, source_blob_name, batch_size): 
    batch = [] 
    batch_id = 1 
    client = storage.Client() 
    bucket = client.bucket(source_bucket) 
    blob = bucket.blob(source_blob_name) 
  
    with blob.open('r', encoding = 'utf-8') as b: 
        for line in b: 
            line = line.rstrip('\n') 
            if not line: 
                continue 
            batch.append(line) 
            if len(batch) == batch_size: 
                yield batch_id, batch 
                batch = [] 
                batch_id += 1 
    if batch: 
        yield batch_id, batch 
 
def upload_to_gcs(source_bucket, source_blob_name, batch_size, destination_bucket): 
    client = storage.Client() 
    dst_bucket = client.bucket(destination_bucket) 
    
    create_objects = [] 
    
    for batch_id, batch in create_batches(source_bucket, source_blob_name, batch_size): 
        data = {'batch_id': batch_id, 'artist_names': batch} 
        json_file = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8') 
        dst_blob_name = f'batch_{batch_id:06d}.json' 
        dst_blob = dst_bucket.blob(dst_blob_name)
        try: 
            dst_blob.upload_from_string( 
                json_file, 
                content_type = 'application/json',
                if_generation_match = 0
                ) 
        except PreconditionFailed: 
            continue 
        
        create_objects.append(dst_blob_name) 
    return create_objects

if __name__ == '__main__':
    source_bucket = 'spotify_kworb_artist_names' 
    source_blob_name = 'artist_names.txt'           
    destination_bucket = 'artist_names_batch'        
    batch_size = 500 

created = upload_to_gcs(
    source_bucket=source_bucket,
    source_blob_name=source_blob_name,
    destination_bucket=destination_bucket,
    batch_size=batch_size
)