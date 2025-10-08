from fastapi import APIRouter, UploadFile, Depends, Header, HTTPException
import httpx
from sqlalchemy.orm import Session
from ..models import File
from ..schemas import FileRead
from ..services.minio_client import minio_client, MINIO_BUCKET
from ..services.kafka_client import send_event
from ..db import get_db
import os
from dotenv import load_dotenv
import io

load_dotenv()
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001")

router = APIRouter()


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


@router.post("/upload", response_model=FileRead)
async def upload_file(
    file: UploadFile,
    user=Depends(verify_user),
    db: Session = Depends(get_db),
):
    owner_id = user["id"]

    content = await file.read()

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=file.filename,
        data=io.BytesIO(content),
        length=len(content),
        content_type=file.content_type or "application/octet-stream"
    )
    print(f"🟢 File uploaded to MinIO: {file.filename}")

    db_file = File(name=file.filename, owner_id=owner_id)
    db.add(db_file)
    db.commit()
    db.refresh(db_file)
    print(f"🟢 File saved in DB with id {db_file.id}")

    event = {
        "file_id": db_file.id,
        "filename": db_file.name,
        "owner_id": owner_id
    }
    try:
        send_event("file_uploaded", key=str(db_file.id), value=event)
        print(f"➡️ file_uploaded event sent for file id {db_file.id}")
    except Exception as e:
        print(f"❌ Failed to send file_uploaded event: {e}")

    return db_file


@router.get("/", response_model=list[FileRead])
def list_files(db: Session = Depends(get_db)):
    return db.query(File).all()
