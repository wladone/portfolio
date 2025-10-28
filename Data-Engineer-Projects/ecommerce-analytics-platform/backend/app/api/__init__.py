"""Public API routers."""

from fastapi import APIRouter

from . import router as root_router

api_router = APIRouter()
api_router.include_router(root_router.router)

__all__ = ["api_router"]
