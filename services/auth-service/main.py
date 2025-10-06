import os
import time
from fastapi import FastAPI, Depends, HTTPException, status, Request, Body
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from passlib.context import CryptContext
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.exc import OperationalError
import jwt
import redis
from dotenv import load_dotenv

#region ENV
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_SECONDS = int(os.getenv("ACCESS_TOKEN_EXPIRE_SECONDS", 3600))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
RATE_LIMIT = int(os.getenv("RATE_LIMIT", 10))
RATE_WINDOW = int(os.getenv("RATE_WINDOW", 60))

# region database
def wait_for_db(engine, retries=10, delay=3):
    for i in range(retries):
        try:
            with engine.connect():
                print("✅ Database is ready")
                return
        except OperationalError:
            print(f"⏳ Waiting for database... {i+1}/{retries}")
            time.sleep(delay)
    raise Exception("❌ Database not ready after retries")

Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)

wait_for_db(engine)
Base.metadata.create_all(bind=engine)

# region hasing
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)

# region JWT
def create_access_token(data: dict, expires_delta: int = ACCESS_TOKEN_EXPIRE_SECONDS):
    to_encode = data.copy()
    expire = int(time.time()) + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

#region rate limiting
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def rate_limiter(ip: str):
    key = f"rate:{ip}"
    count = r.get(key)
    if count is None:
        r.set(key, 1, ex=RATE_WINDOW)
    elif int(count) < RATE_LIMIT:
        r.incr(key)
    else:
        raise HTTPException(status_code=429, detail="Too Many Requests")

# region FastAPI
app = FastAPI()

class UserCreate(BaseModel):
    username: str
    password: str

@app.post("/register")
def register(
    user: UserCreate = Body(...),
    request: Request = None,
    db: Session = Depends(get_db)
):
    rate_limiter(request.client.host)
    db_user = db.query(User).filter(User.username == user.username).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    new_user = User(username=user.username, password_hash=hash_password(user.password))
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return {"msg": "User registered successfully"}


@app.post("/login")
def login(
    user: UserCreate = Body(...),
    request: Request = None,
    db: Session = Depends(get_db)
):
    rate_limiter(request.client.host)
    db_user = db.query(User).filter(User.username == user.username).first()
    if not db_user or not verify_password(user.password, db_user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    token = create_access_token({"sub": db_user.username})
    return {"access_token": token, "token_type": "bearer"}

