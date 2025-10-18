from fastapi import FastAPI
from routers import items  # items.py에서 라우터 임포트

app = FastAPI()

# 라우터 등록
app.include_router(items.router)
