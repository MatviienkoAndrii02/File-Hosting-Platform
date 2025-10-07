from fastapi import APIRouter, UploadFile, Depends, Header, HTTPException
import httpx
from sqlalchemy.orm import Session
from ..models import File
from ..schemas import FileCreate, FileRead
from ..services.minio_client import minio_client, MINIO_BUCKET
from ..db import get_db
import os
from dotenv import load_dotenv
import io

load_dotenv()
AUTH_SERVICE_URL =  os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001")


async def verify_user(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{AUTH_SERVICE_URL}/verify-token",
            headers={"Authorization": authorization}
        )
    
    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    return resp.json() 

router = APIRouter()

@router.post("/upload", response_model=FileRead)
async def upload_file(
    file: UploadFile,
    user=Depends(verify_user),
    db: Session = Depends(get_db),
):
    owner_id = user["id"]

    # 1️⃣ Зчитуємо файл у байти, бо UploadFile — це StreamingFile
    content = await file.read()

    # 2️⃣ Завантажуємо у MinIO
    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=file.filename,
        data=io.BytesIO(content),  # треба об'єкт з методом read()
        length=len(content),
        content_type=file.content_type or "application/octet-stream"
    )

    # 3️⃣ Зберігаємо у PostgreSQL
    db_file = File(name=file.filename, owner_id=owner_id)
    db.add(db_file)
    db.commit()
    db.refresh(db_file)

    return db_file

@router.get("/", response_model=list[FileRead])
def list_files(db: Session = Depends(get_db)):
    return db.query(File).all()
