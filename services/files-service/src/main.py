from fastapi import FastAPI
from contextlib import asynccontextmanager
from minio.error import S3Error
from .services.minio_client import minio_client, MINIO_BUCKET
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base
from .routers import files

async def init_minio():
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            print(f"✅ Created bucket: {MINIO_BUCKET}")
        else:
            print(f"ℹ️ Bucket {MINIO_BUCKET} already exists")
    except S3Error as e:
        print(f"❌ MinIO error: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_minio()
    yield


app = FastAPI(title="Files Service")

app.include_router(files.router, prefix="/files", tags=["files"])
