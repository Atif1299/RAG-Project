from fastapi import APIRouter, HTTPException
from ..services.progress_service import progress_service

router = APIRouter()

@router.get("/progress/{user_id}")
async def get_progress(user_id: str):
    progress = progress_service.get_progress(user_id)
    if not progress:
        raise HTTPException(status_code=404, detail="No progress found for this user.")
    return progress 