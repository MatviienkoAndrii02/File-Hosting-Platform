from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv()

# Беремо дані з середовища
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "files")

# Підключення до MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False  # HTTP
)

# Створюємо bucket, якщо його немає
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    print(f"Bucket '{MINIO_BUCKET}' створено")
else:
    print(f"Bucket '{MINIO_BUCKET}' вже існує")
