from typing import Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query

from app.db import get_pool
from app.queries import (
    archive_old_jobs,
    cancel_job,
    enqueue,
    get_dead_letter,
    get_job,
    list_jobs,
    retry_dead_letter,
)
from app.schemas import JobCreate, JobRead

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _to_read(row: dict) -> JobRead:
    return JobRead(**row)


@router.post("", response_model=JobRead, status_code=201)
async def create_job(body: JobCreate):
    pool = get_pool()
    # Validate job_type early for nicer error (worker also validates)
    row = await enqueue(
        pool,
        job_type=body.job_type,
        payload=body.payload,
        idempotency_key=body.idempotency_key,
        max_attempts=body.max_attempts,
        run_after=body.run_after,
        priority=body.priority,
    )
    if not row:
        raise HTTPException(status_code=500, detail="enqueue failed")
    # If idempotency hit, we returned existing; status code should be 200
    # But spec says return existing status instead of erroring — we do that.
    return _to_read(row)


@router.get("", response_model=dict)
async def list_jobs_ep(
    status: Optional[str] = Query(default=None),
    job_type: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    if status and status not in ("pending", "leased", "succeeded", "failed", "dead"):
        raise HTTPException(status_code=400, detail="invalid status")
    pool = get_pool()
    rows, total = await list_jobs(pool, status=status, job_type=job_type, limit=limit, offset=offset)
    return {"jobs": [_to_read(r).model_dump(mode="json") for r in rows], "total": total}


@router.get("/{job_id}", response_model=JobRead)
async def get_job_ep(job_id: UUID):
    pool = get_pool()
    row = await get_job(pool, job_id)
    if row:
        return _to_read(row)
    dl = await get_dead_letter(pool, job_id)
    if dl:
        # Return synthetic view: dead-letter jobs are not in jobs table anymore? In our model failed jobs stay + also in DLQ.
        # But if cancelled+deleted case, show DLQ.
        raise HTTPException(status_code=410, detail={"msg": "job in dead-letter", "dead_letter": dl})
    raise HTTPException(status_code=404, detail="not found")


@router.delete("/{job_id}")
async def cancel_job_ep(job_id: UUID):
    pool = get_pool()
    row = await get_job(pool, job_id)
    if not row:
        dl = await get_dead_letter(pool, job_id)
        if dl:
            raise HTTPException(status_code=410, detail="already dead-lettered")
        raise HTTPException(status_code=404, detail="not found")
    if row["status"] != "pending":
        return {"status": "no-op", "reason": f"job status is {row['status']}, only pending can be cancelled", "job": _to_read(row).model_dump(mode="json")}
    cancelled = await cancel_job(pool, job_id)
    if not cancelled:
        raise HTTPException(status_code=409, detail="cancel race lost")
    return {"status": "cancelled", "job": _to_read(cancelled).model_dump(mode="json")}


@router.post("/{job_id}/retry", response_model=JobRead)
async def retry_job_ep(job_id: UUID):
    pool = get_pool()
    row = await retry_dead_letter(pool, job_id)
    if not row:
        raise HTTPException(status_code=404, detail="not found or not retryable (only failed/dead-letter can be retried)")
    return _to_read(row)


@router.post("/admin/archive")
async def archive_ep(retention_days: int = 30):
    """Archive succeeded jobs older than N days to jobs_archive (retention)."""
    if retention_days < 1:
        raise HTTPException(status_code=400, detail="retention_days must be >=1")
    pool = get_pool()
    moved = await archive_old_jobs(pool, retention_days)
    return {"archived": moved, "retention_days": retention_days}
