from fastapi import APIRouter, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from app.db import get_pool
from app.queries import queue_depth

router = APIRouter(tags=["metrics"])

registry = CollectorRegistry()
depth_gauge = Gauge("queue_depth", "Jobs by status", ["status"], registry=registry)
jobs_processed = Counter("jobs_processed_total", "Jobs processed", ["outcome"], registry=registry)
job_duration = Histogram("job_duration_seconds", "Job duration", registry=registry)


@router.get("/metrics")
async def metrics():
    # refresh gauges from DB
    try:
        pool = get_pool()
        depth = await queue_depth(pool)
        for k, v in depth.items():
            depth_gauge.labels(status=k).set(v)
    except Exception:
        pass
    data = generate_latest(registry)
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)
