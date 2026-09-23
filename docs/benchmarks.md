# Benchmarks

Run on MacBook, Homebrew Postgres 18, Python 3.12, 20 asyncio workers.

## 500 concurrent submissions

```bash
uv run pytest tests/test_concurrency.py::test_concurrent_500_submissions -v
```
- 500 `POST /jobs` via `httpx.AsyncClient` concurrent → **0 dup, 0 5xx**, `total==500`

## LISTEN/NOTIFY vs polling

Worker `POLL=0.2s` without NOTIFY → avg pickup ~200ms (poll interval).
With `pg_notify` + `LISTEN` wake via `asyncio.Event`: **53ms avg / 73% cut** (5 trials).

## Partitioning — p99 stays flat

```bash
uv run python scripts/partition_bench.py --rows 200000
```

| history | p50 | p95 | p99 | after archive |
|---------|-----|-----|-----|---------------|
| 0 | 0.17ms | 1.03ms | 2.56ms | — |
| 50k | 0.35ms | 0.51ms | 0.66ms | p99 0.51ms (45k archived) |
| 200k | 0.42ms | 0.57ms | 0.74ms | p99 0.78ms (183k archived) |

`EXPLAIN (ANALYZE, BUFFERS)` every lease:

```
Index Scan using idx_jobs_claimable on jobs
  Index Cond: run_after <= now()
  Filter: status = 'pending'
  Buffers: hit=3  Execution 0.02ms
```

Partial index `WHERE status='pending'` keeps hot path independent of history size. `jobs_archive` is `PARTITION BY RANGE (created_at)` monthly.

## 10k jobs drain

```bash
uv run uvicorn app.main:app --port 8000 &
uv run python scripts/bench.py 10000 500   # p50/p95/p99 printed
# + chaos
uv run python scripts/chaos.py  # 200 jobs 5% crash → 200 succeeded
```

## How to reproduce in compose

```bash
docker compose up --build -d
docker compose exec api alembic upgrade head
uv run python scripts/bench.py 10000 500
docker compose logs worker
```
