# Architecture

## Data Flow

```mermaid
flowchart TB
    C[Client] -->|POST /jobs {type, payload, idempotency_key}| API[FastAPI API<br/>+ reaper loop]
    API -->|INSERT + pg_notify jobs_channel| PG[(Postgres<br/>jobs hot table)]
    PG -->|LISTEN| W1[Worker 1]
    PG -->|LISTEN| W2[Worker 2]
    PG -->|LISTEN| WN[Worker N]
    W1 -->|lease: FOR UPDATE SKIP LOCKED| PG
    W1 -->|heartbeat<br/>leased_until = now()+8s<br/>every 2.6s| PG
    W1 -->|mark_succeeded / mark_failed| PG
    PG -->|reap: leased_until < now()| PG
    PG -->|archive_old_jobs 30d| ARC[(jobs_archive<br/>PARTITION BY RANGE created_at<br/>monthly p2026_07..10 + default)]
```

## Lease Algorithm (core correctness)

Single statement, atomic, no SELECT-then-UPDATE race:

```sql
WITH next_job AS (
  SELECT id FROM jobs
  WHERE status = 'pending' AND run_after <= now()
  ORDER BY priority DESC, run_after ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED   -- never blocks on row another worker holds
)
UPDATE jobs SET
  status='leased', leased_by=:wid,
  leased_until=now()+interval '8 seconds',
  attempts=attempts+1
FROM next_job WHERE jobs.id=next_job.id
RETURNING *;
```

- 50 workers racing → each gets distinct row
- `SKIP LOCKED` is the standard Oban-style Postgres queue pattern

## Failure Model

- **Worker crash** → `leased_until` expires → reaper `UPDATE status='pending' WHERE leased_until < now()` every 2s
- **Long job** → heartbeat renews `leased_until` every `lease/3` so alive workers never reaped
- **DB clock skew** → all comparisons use DB `now()`, never app clock

## Partitioning

- Hot table `jobs` stays small via `archive_old_jobs(retention_days)` moving `succeeded` older than N days → `jobs_archive`
- `jobs_archive` is `PARTITION BY RANGE (created_at)` with monthly children; `idx_jobs_claimable WHERE status='pending'` keeps lease p99 flat even as history grows
- `pgbouncer` transaction pooling: 500 app conns → 25 Postgres conns
