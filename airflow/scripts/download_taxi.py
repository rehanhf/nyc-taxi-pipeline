import sys
import boto3
import requests
from botocore.client import Config
import os
from botocore.exceptions import ClientError

def download_and_upload(year_month):
    # MinIO configuration
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    
    bucket_name = 'nyc-taxi-raw'
    object_key = f'{year_month}.parquet'
    
    # S3 Client
    s3_client = boto3.client(
        's3',
        endpoint_url=f'http://{minio_endpoint}',
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # --- CHECK EXISTING FILE ---
    try:
        obj = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        size = obj['ContentLength']
        if size > 1000000: # If larger than 1MB, it's valid
            print(f"File {object_key} exists ({size / (1024**2):.2f} MB). Skipping.")
            return True
        else:
            print(f"Found corrupted/empty file ({size} bytes). Re-downloading...")
    except ClientError:
        print(f"File {object_key} not found. Downloading...")

    # --- DOWNLOAD LOGIC ---
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
    print(f"Downloading from {url}")
    
    # FIX: Add Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Stream the download
    response = requests.get(url, headers=headers, stream=True)
    
    if response.status_code != 200:
        raise Exception(f"Failed to download: HTTP {response.status_code}")
    
    # Upload to MinIO
    print(f"Uploading to minio://{bucket_name}/{object_key}")
    s3_client.upload_fileobj(response.raw, bucket_name, object_key)
    
    # --- FINAL VERIFICATION ---
    obj_metadata = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    uploaded_size = obj_metadata['ContentLength']
    
    print(f"Final Size in MinIO: {uploaded_size / (1024**2):.2f} MB")
    
    if uploaded_size < 1000000: # Less than 1MB
        raise Exception(f"Download failed! File is too small ({uploaded_size} bytes). CloudFront is blocking us.")
        
    return True

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python download_taxi.py YYYY-MM")
        sys.exit(1)
    
    year_month = sys.argv[1]
    
    try:
        download_and_upload(year_month)
        print(f"SUCCESS: {year_month} processed")
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)