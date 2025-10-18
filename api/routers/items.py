from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

router = APIRouter(
    prefix="/items",  # 모든 경로 앞에 /items 붙음
    tags=["items"]    # Swagger 문서에서 그룹화용 태그
)

class Item(BaseModel):
    name: str
    price: float
    in_stock: bool

@router.put("/{item_id}")
def update_item(
    item_id: int,
    item: Item,
    authorization: str = Header(None),
    x_custom_header: str = Header(None)
):
    if authorization != "Bearer mysecrettoken123":
        raise HTTPException(status_code=401, detail="Unauthorized")

    return {
        "item_id": item_id,
        "updated_item": item,
        "auth": authorization,
        "custom": x_custom_header
    }

@router.get("/")
def get_items():
    return [{"item_id": 1, "name": "Item One"}, {"item_id": 2, "name": "Item Two"}]

@router.get("/{item_id}")
def get_item(item_id: int):
    return {"item_id": item_id, "name": f"Item {item_id}"}
