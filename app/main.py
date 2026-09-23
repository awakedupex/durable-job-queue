import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.jobs import router as jobs_router
from app.api.metrics import router as metrics_router
from app.db import close_pool, get_pool, open_pool
from app.worker.reaper import reaper_loop

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = get_pool()
    await open_pool()
    # start reaper in api process as well (handles crash even if workers down for small deploys)
    stop = asyncio.Event()
    task = asyncio.create_task(reaper_loop(pool, stop))
    logger.info("api started, reaper running")
    yield
    stop.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await close_pool()


app = FastAPI(title="Durable Job Queue", lifespan=lifespan)

app.include_router(jobs_router)
app.include_router(health_router)
app.include_router(metrics_router)
