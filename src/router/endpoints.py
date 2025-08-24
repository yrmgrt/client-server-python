from fastapi import APIRouter

from router.screener import router as screener_router
from router.admin import router as admin_router

router = APIRouter()

router.include_router(router=screener_router)
router.include_router(router=admin_router)
