import os
import json
from io import BytesIO
import logging
from minio import Minio
from minio.error import S3Error

def get_minio_client() -> Minio:
    """
    Initializes and returns a Minio client.
    Pulls connection details from environment variables (useful in Docker)
    or falls back to defaults for local testing.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password")
    # In local docker setup, secure is False (HTTP)
    secure_env = os.getenv("MINIO_SECURE", "False").lower()
    secure = secure_env in ("true", "1", "t")

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
    return client

def upload_json_to_minio(bucket_name: str, object_name: str, data_dict: dict):
    """
    Converts a Python dictionary to JSON and uploads it to the specified MinIO bucket.
    If the bucket doesn't exist, it attempts to create it.
    """
    client = get_minio_client()
    
    try:
        # Ensure bucket exists
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            logging.info(f"Created bucket '{bucket_name}' in MinIO.")

        # Convert dict to JSON string then to bytes
        json_bytes = json.dumps(data_dict, ensure_ascii=False).encode('utf-8')
        bytes_io = BytesIO(json_bytes)
        
        # Upload
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=bytes_io,
            length=len(json_bytes),
            content_type="application/json"
        )
        logging.info(f"Successfully uploaded {object_name} to bucket '{bucket_name}'.")
        
    except S3Error as exc:
        logging.error(f"S3Error occurred while uploading {object_name} to MinIO: {exc}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error uploading to MinIO: {e}")
        raise
