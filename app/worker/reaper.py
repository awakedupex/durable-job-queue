import asyncio
import logging

from psycopg_pool import AsyncConnectionPool

from app.config import settings
from app.queries import reap_expired_leases

logger = logging.getLogger(__name__)


async def reaper_loop(pool: AsyncConnectionPool, stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.reaper_interval_seconds)
            break
        except asyncio.TimeoutError:
            try:
                n = await reap_expired_leases(pool)
                if n:
                    logger.info("reaper reclaimed %s jobs", n)
            except Exception as e:
                logger.warning("reaper error %s", e)
