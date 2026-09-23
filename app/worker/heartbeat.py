import asyncio
import logging
import uuid

from psycopg_pool import AsyncConnectionPool

from app.queries import heartbeat_renew

logger = logging.getLogger(__name__)


async def heartbeat_loop(pool: AsyncConnectionPool, job_id: uuid.UUID, lease_seconds: int, stop_event: asyncio.Event):
    interval = max(1.0, lease_seconds / 3)
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            try:
                ok = await heartbeat_renew(pool, job_id, lease_seconds)
                if not ok:
                    logger.warning("heartbeat_renew no row job_id=%s", job_id)
                    break
                logger.debug("heartbeat renewed job_id=%s", job_id)
            except Exception as e:
                logger.warning("heartbeat failed job_id=%s error=%s", job_id, e)
