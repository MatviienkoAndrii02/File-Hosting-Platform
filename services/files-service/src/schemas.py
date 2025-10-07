from pydantic import BaseModel

class FileCreate(BaseModel):
    name: str
    tags: str = ""

class FileRead(BaseModel):
    id: int
    name: str
    owner_id: int
    tags: str