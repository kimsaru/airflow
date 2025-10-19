from fastapi import APIRouter

router = APIRouter()

@router.get("/aaa")
async def read_aaa():
    return {"message": "This is aaa router"}
