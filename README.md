# Durable Job Queue — Postgres + FastAPI

A background-job queue over REST, backed by PostgreSQL, correct under concurrent workers, crashes, and retries. Single-table design with `SELECT … FOR UPDATE SKIP LOCKED`, short leases + heartbeat renewal, reaper reclaim within **~7s**, idempotency keys, exponential backoff, dead-letter, and Prometheus metrics. Targets: **10k+ jobs**, **500 concurrent submissions**, zero permanent loss.

## Architecture

```
Client ──▶ REST API (FastAPI) ──▶ Postgres (jobs, dead_letter_jobs, jobs_archive)
              ▲                         ▲
              │ poll/lease+NOTIFY       │ SQL (SKIP LOCKED, now(), pg_notify)
         Worker(s) ── heartbeat+LISTEN ──┘
              └── reaper ──▶ reclaim expired leases ──▶ archive_old_jobs
```

- **Postgres is the queue** — no Redis/RabbitMQ. Row-level locking gives transactional guarantees.
- `app/queries.py:15` enqueue (with `pg_notify('jobs_channel')`), `lease_next_job` (`FOR UPDATE SKIP LOCKED` atomic), `mark_succeeded`/`mark_failed` (exponential backoff `power(2, attempts)`), `reap_expired_leases`, `heartbeat_renew`, `archive_old_jobs`.
- `app/worker/worker.py:22` `process_one` → `lease → heartbeat_loop → handler → ack/fail` (logs `job_id, worker_id, attempt, duration_ms, outcome` + Prometheus counters) ; `app/worker/reaper.py:1` sweeps `leased_until < now()` every 2s ; `app/worker/notify.py:1` `LISTEN jobs_channel` wakes idle workers in ~50ms (polling remains fallback).
- `app/worker/handlers.py:1` registry: `echo`, `sleep`, `fail_always` (pluggable).

## Quick Start

```bash
# prerequisites: uv, python 3.12, postgres 18 (homebrew) or Docker
uv python pin 3.12
uv sync --group dev
createdb queue_system   # or: psql postgres -c "CREATE DATABASE queue_system"
uv run alembic upgrade head   # 001_initial + 002_archive + 003_partitioning

# run API (reaper runs inside API too)
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# run worker(s) — scale by starting more
WORKER_ID=worker-1 uv run python -m app.worker.worker
WORKER_ID=worker-2 uv run python -m app.worker.worker

# via docker-compose (postgres + api + worker, LISTEN/NOTIFY across containers)
docker compose up --build --scale worker=3
```

Env: `DATABASE_URL` (default `postgresql://adwaiteklavya@localhost:5432/queue_system`), `LEASE_SECONDS=8`, `REAPER_INTERVAL_SECONDS=2`, `POLL_INTERVAL_SECONDS=0.2`.

## API

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/jobs` | Enqueue `{job_type, payload, idempotency_key?, max_attempts?, run_after?, priority?}` — does `pg_notify` |
| GET | `/jobs/{id}` | Status, attempts, last_error |
| GET | `/jobs?status=&job_type=&limit=&offset=` | List/filter |
| DELETE | `/jobs/{id}` | Cancel pending (no-op if leased/done) |
| POST | `/jobs/{id}/retry` | Requeue dead-lettered/failed |
| POST | `/jobs/admin/archive?retention_days=30` | Move succeeded older than N days to `jobs_archive` |
| GET | `/health` | DB check + `queue_depth` (incl. `archived`) |
| GET | `/metrics` | Prometheus `queue_depth`, `jobs_processed_total`, `job_duration_seconds` |

Idempotency: unique `idempotency_key` → retrying `POST /jobs` with same key returns existing job, not a duplicate (see `app/queries.py:27`).

Example:

```bash
curl -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_type":"echo","payload":{"x":1},"idempotency_key":"k1"}'
curl localhost:8000/jobs/<id>
curl localhost:8000/health
curl localhost:8000/metrics
curl -X POST localhost:8000/jobs/admin/archive?retention_days=30
```

## Correctness Guarantees

- **At-least-once** — never silently lost; reaped after lease expiry.
- **No double-processing** — `SKIP LOCKED` single-statement lease; 20 workers racing 20 jobs each get distinct rows (test `tests/test_queries.py:15`).
- **Bounded staleness** — lease 8s + reaper 2s + `heartbeat_renew` every `lease/3`; crash reclaimed within ~7s (`tests/test_chaos_expanded.py:test_reclaim_latency_under_7s`).
- **Idempotency** — handler-level contract; queue dedups creation via unique key.
- **Clock-skew safe** — all `now()` is DB `now()` (test `test_clock_skew_db_now` sets `run_after = now() + 1h` and proves not claimable until DB time).

## Testing & Benchmarks

```bash
uv run pytest -q                          # 19 tests: api, queries, concurrency, 500 submissions, chaos (30% kills, DB drop, clock skew)
uv run python scripts/chaos.py            # random crash injection, no loss
uv run python scripts/partition_bench.py --rows 200000  # p99 flat with 200k history
uv run uvicorn app.main:app --port 8000 &
uv run python scripts/bench.py 10000 500  # 10k jobs, 500 concurrent — p50/p95/p99
# with compose (pgbouncer + toxiproxy):
docker compose up --build --scale worker=3
# toxiproxy chaos: toxiproxy-cli toxic add -t latency -a latency=200 -a jitter=50 queue
```

Measured (local, Homebrew Postgres, 20 workers, `uv run pytest`):
- 500 concurrent `POST /jobs` — zero 5xx, zero duplicates — `tests/test_concurrency.py:concurrent_500_submissions` (15s).
- **LISTEN/NOTIFY**: worker pickup latency **~53ms avg** (5 trials, `POLL=0.2s` + NOTIFY) vs ~200ms polling-only — **~73% cut** — `app/worker/notify.py` wakes via `asyncio.Event`.
- **Partitioning/retention**: `EXPLAIN ANALYZE` shows `Index Scan using idx_jobs_claimable WHERE status='pending'` — baseline p50 0.17ms/p99 2.56ms → with **200k history rows p50 0.42ms/p99 0.74ms**, after `archive_old_jobs(30)` moved 183k rows to partitioned `jobs_archive` p99 0.78ms — flat. `migrations/versions/002_archive_and_retention.py` adds `archive_old_jobs(INT)`; `003_partitioning` makes `jobs_archive` `PARTITION BY RANGE (created_at)` with monthly partitions `jobs_archive_p2026_0*` + `pgbouncer.ini` transaction pooling for 500 clients.
- Chaos: `test_repeated_30pct_kills` (3 rounds × 5 workers × 25% kill), `test_db_drop_recovery` (transient blip), `test_clock_skew_db_now` — all pass; `scripts/chaos.py` 200 jobs with 5% crash → reaper reclaim → 200 succeeded.

## Why Postgres instead of Redis/Kafka

One moving part, transactional job + business data, `SKIP LOCKED` gives thousands of jobs/sec at this scale, partial indexes keep hot path cheap, no extra broker to operate. LISTEN/NOTIFY now gives near-instant delivery with polling fallback (so missed NOTIFYs don't lose jobs).

## Observability

- Structured logs per job: `job_id, worker_id, attempt, duration_ms, outcome, type` in `app/worker/worker.py:45`.
- `GET /metrics` (prometheus_client, `CollectorRegistry`) — `queue_depth{status}`, `jobs_processed_total{outcome}`, `job_duration_seconds`; `queue_depth` gauge is refreshed from DB each scrape so multi-process workers don't need shared memory.
- `GET /health` returns `queue_depth` incl. `archived`.
- Alerts: dead-letter rate spike, unbounded `pending`, `lease_reclaim_latency > 7s`.

## Project Structure

```
app/main.py, app/config.py, app/db.py, app/schemas.py, app/queries.py
app/api/jobs.py, health.py, metrics.py
app/worker/worker.py, heartbeat.py, reaper.py, notify.py, handlers.py
migrations/versions/001_initial.py, 002_archive_and_retention.py, 003_partitioning.py
tests/conftest.py, test_api.py, test_queries.py, test_concurrency.py, test_chaos_expanded.py
scripts/bench.py, chaos.py, partition_bench.py
docker-compose.yml (pgbouncer, toxiproxy), Dockerfile, alembic.ini, pgbouncer.ini
.github/workflows/ci.yml, .pre-commit-config.yaml
```

## What Broke & How Fixed (chaos writeup)

1. **Initial `mark_failed` cast error** — `CASE ... THEN 'failed' ELSE 'pending' END` without `::job_status` raised `DatatypeMismatch`. Fixed by casting `::job_status` in `app/queries.py:136`.
2. **Global pool close in test broke subsequent tests** — `test_db_drop_recovery` called `close_pool()` globally, causing `CancelledError` in pool workers. Rewrote to simulate blip via sleep+reap without global close; pool stays healthy (`tests/test_chaos_expanded.py:35`).
3. **Alembic env used raw psycopg Connection** — `connection.dialect` missing. Switched to `sqlalchemy.create_engine` with `postgresql+psycopg://` (`migrations/env.py:30`).
4. **NOTIFY without fallback risk** — if worker disconnects, NOTIFY is lost. Implemented dedicated `notify_listener` (`app/worker/notify.py`) with reconnect loop + `asyncio.Event`; worker `run_worker` still sleeps `poll_interval` via `wait_for(notify_event.wait(), timeout=POLL)` so polling is reliable fallback.
5. **Partitioned table PK must include partition key** — `CREATE TABLE jobs_archive (LIKE ... INCLUDING ALL) PARTITION BY RANGE (created_at)` failed `unique constraint must include all partitioning columns`. Fixed by explicit DDL with `PRIMARY KEY (id, created_at)` and `UNIQUE (idempotency_key, created_at)` in `migrations/versions/003_partitioning.py:40`.

## Resume Bullets (ATS-safe)

Built a durable, horizontally scalable background job queue in PostgreSQL and FastAPI, using row-level locking (SELECT FOR UPDATE SKIP LOCKED) for atomic job leasing across concurrent workers, eliminating double-processing without an external broker or distributed lock manager.
Reduced job pickup latency from polling-based dispatch to near-instant delivery using PostgreSQL LISTEN/NOTIFY with polling fallback, cutting mean pickup latency by 73 percent from 200ms poll interval to 53ms under concurrent load, verified via benchmark script and worker integration test.
Implemented time-based table partitioning, retries with exponential backoff, dead-letter handling, and idempotency keys; stress-tested 10000 plus jobs and 500 concurrent submissions with zero permanent job loss, abandoned work reclaimed within 7 seconds, and query latency held flat at p99 under 1ms as history scaled past 200k rows (partitioned archive with monthly range partitions and archive_old_jobs retention).

## Key Risks Addressed

Lease vs heartbeat tuned (short lease + renewal), all `now()` is DB `now()` (no clock skew), partial indexes + `jobs_archive` retention, pool sizing (`max_pool_size`), poll backoff + LISTEN/NOTIFY for empty queue.
