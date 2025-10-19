from fastapi import FastAPI
from routers import items  # items.py에서 라우터 임포트
from routers import aaa    # 새 라우터 추가

app = FastAPI()

# 라우터 등록 --
app.include_router(items.router)
app.include_router(aaa.router)  # 새 라우터 등록