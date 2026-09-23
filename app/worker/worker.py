import asyncio
import logging
import os
import uuid

from app.config import settings
from app.db import get_pool, open_pool
from app.queries import lease_next_job, mark_failed, mark_succeeded
from app.worker.handlers import get_handler
from app.worker.heartbeat import heartbeat_loop
from app.worker.notify import notify_listener
from app.worker.reaper import reaper_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


async def process_one(pool, worker_id: str) -> bool:
    job = await lease_next_job(pool, worker_id, lease_seconds=settings.lease_seconds)
    if not job:
        return False

    job_id = job["id"]
    job_type = job["job_type"]
    payload = job["payload"]
    attempt = job["attempts"]
    import time

    start = time.monotonic()
    logger.info(
        "leased job_id=%s worker=%s type=%s attempt=%s",
        job_id,
        worker_id,
        job_type,
        attempt,
    )

    stop_event = asyncio.Event()
    hb_task = asyncio.create_task(heartbeat_loop(pool, job_id, settings.lease_seconds, stop_event))
    outcome = "succeeded"
    try:
        handler = get_handler(job_type)
        await handler(payload)
        await mark_succeeded(pool, job_id)
    except Exception as e:
        outcome = "failed"
        logger.warning("failed job_id=%s worker=%s attempt=%s error=%s", job_id, worker_id, attempt, e)
        await mark_failed(pool, job_id, str(e))
    finally:
        stop_event.set()
        hb_task.cancel()
        try:
            await hb_task
        except asyncio.CancelledError:
            pass
        duration_ms = int((time.monotonic() - start) * 1000)
        # structured log + metrics: job_id, worker_id, attempt, duration_ms, outcome
        logger.info(
            "job_done job_id=%s worker=%s attempt=%s duration_ms=%s outcome=%s type=%s",
            job_id,
            worker_id,
            attempt,
            duration_ms,
            outcome,
            job_type,
        )
        try:
            from app.api.metrics import job_duration, jobs_processed

            job_duration.observe(duration_ms / 1000)
            jobs_processed.labels(outcome=outcome).inc()
        except Exception:
            pass
    return True


async def run_worker(worker_id: str | None = None):
    wid = worker_id or settings.worker_id or f"worker-{uuid.uuid4().hex[:6]}"
    pool = get_pool()
    await open_pool()
    stop = asyncio.Event()
    notify_event = asyncio.Event()
    reaper_task = asyncio.create_task(reaper_loop(pool, stop))
    notify_task = asyncio.create_task(notify_listener(notify_event, stop))
    logger.info("worker %s started lease=%ss poll=%ss (LISTEN/NOTIFY enabled)", wid, settings.lease_seconds, settings.poll_interval_seconds)
    try:
        while True:
            did = await process_one(pool, wid)
            if not did:
                # sleep until either poll timeout OR NOTIFY wake
                notify_event.clear()
                try:
                    await asyncio.wait_for(notify_event.wait(), timeout=settings.poll_interval_seconds)
                    # woken by NOTIFY -> loop immediately to lease
                    continue
                except asyncio.TimeoutError:
                    pass
    except asyncio.CancelledError:
        logger.info("worker %s stopping", wid)
    finally:
        stop.set()
        notify_event.set()  # wake notify listener
        for t in (reaper_task, notify_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    wid = os.environ.get("WORKER_ID", settings.worker_id)
    try:
        asyncio.run(run_worker(wid))
    except KeyboardInterrupt:
        pass
