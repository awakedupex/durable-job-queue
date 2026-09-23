from fastapi import APIRouter

from app.db import get_pool
from app.queries import queue_depth

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    pool = get_pool()
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchone()
        depth = await queue_depth(pool)
        return {"status": "ok", "queue_depth": depth}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}


@router.get("/")
async def root():
    return {"status": "ok", "service": "queue_system"}
