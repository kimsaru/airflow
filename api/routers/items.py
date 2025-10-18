from fastapi import APIRouter

router = APIRouter(
    prefix="/items",  # 모든 경로 앞에 /items 붙음
    tags=["items"]    # Swagger 문서에서 그룹화용 태그
)

@router.get("/")
def get_items():
    return [{"item_id": 1, "name": "Item One"}, {"item_id": 2, "name": "Item Two"}]

@router.get("/{item_id}")
def get_item(item_id: int):
    return {"item_id": item_id, "name": f"Item {item_id}"}
