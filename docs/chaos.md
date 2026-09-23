# Chaos & Reliability

## What we test

| test | file | what breaks | assert |
|------|------|-------------|--------|
| no double lease | `test_queries::test_lease_atomicity` | 20 workers race 20 jobs | 20 distinct ids |
| reclaim | `test_queries::test_reap_reclaim` | lease 2s → sleep 2.5s | reclaimed 1, re-leasable |
| heartbeat | `test_queries::test_heartbeat` | renew extends, no reap | reap 0 then 1 after expiry |
| backoff/DLQ | `test_queries::test_backoff` | fail 3× with forced `run_after=now()` | status pending→failed, DLQ 1 |
| 200 no-double | `test_concurrency::test_10k_jobs_no_double` | 20 workers drain 200 | 200 succeeded |
| crash <7s | `test_concurrency::test_crash_reclaim_within_7s` | victim lease 3s → reap | elapsed <7s |
| kill-all | `test_concurrency::test_no_permanent_loss` | lease half, kill all, reap | leased 0, succeeded 50 |
| 500 submit | `test_concurrency::test_concurrent_500_submissions` | 500 asyncio posts | 500 ids distinct |
| 30% kills ×3 | `test_chaos_expanded::test_repeated_30pct_kills` | 25% random no-ack 3 rounds | 100 processed |
| DB blip | `test_chaos_expanded::test_db_drop_recovery` | sleep past lease (toxiproxy-like) | reap 1, pool healthy |
| clock skew | `test_chaos_expanded::test_clock_skew_db_now` | run_after +1h vs -1s | DB now() only |
| SLA | `test_chaos_expanded::test_reclaim_latency_under_7s` | lease 5s | <7s |

## Toxiproxy (compose)

`docker-compose.yml` runs `toxiproxy:2.12.0` with `8474` (admin) + `5433` (proxy).

```bash
# create proxy postgres:5432 → toxiproxy:5433
docker compose exec toxiproxy /go/bin/toxiproxy-cli create -l 0.0.0.0:5433 -u postgres:5432 queue

# latency 150ms ±50ms
docker compose exec toxiproxy /go/bin/toxiproxy-cli toxic add -t latency -a latency=150 -a jitter=50 queue
DATABASE_URL=postgresql://adwaiteklavya@toxiproxy:5433/queue_system uv run pytest -k clock_skew

# drop
docker compose exec toxiproxy /go/bin/toxiproxy-cli toxic add -t limit_data -a bytes=1024 queue
docker compose exec toxiproxy /go/bin/toxiproxy-cli toxic remove -n latency queue
```

All leases still use DB `now()`, so clock skew between workers never causes premature reap.

## What broke

See README “What Broke & How Fixed” — `::job_status` cast, global pool close, alembic `dialect`, NOTIFY fallback, partitioned PK.
