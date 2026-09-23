# API Reference

Base `http://localhost:8000`

| Method | Path | Body / Params | Success |
|--------|------|---------------|---------|
| `POST` | `/jobs` | `{job_type, payload, idempotency_key?, max_attempts?=5, priority?=0, run_after?}` | `201 {id, status:pending}` or `200` if idempotent hit |
| `GET` | `/jobs/{id}` | — | `200 JobRead` or `410` if in dead-letter, `404` |
| `GET` | `/jobs` | `?status=pending|leased|succeeded|failed&job_type=&limit=50&offset=0` | `{jobs:[], total}` |
| `DELETE` | `/jobs/{id}` | — | `cancelled` if pending else `no-op` or `410` |
| `POST` | `/jobs/{id}/retry` | — | `200 JobRead` pending or `404` |
| `POST` | `/jobs/admin/archive` | `?retention_days=30` | `{archived:N}` |
| `GET` | `/health` | — | `{status:ok, queue_depth:{pending,leased,…}}` |
| `GET` | `/metrics` | — | Prometheus text |
| `GET` | `/` | — | `{status:ok, service}` |

## Schemas

```json
// JobRead
{
  "id": "uuid",
  "job_type": "echo | sleep | fail_always | your_type",
  "payload": {},
  "status": "pending|leased|succeeded|failed",
  "priority": 0,
  "attempts": 0, "max_attempts": 5,
  "leased_by": "worker-1|null", "leased_until": "2026-09-23T...",
  "run_after": "2026-09-23T...", "last_error": null,
  "created_at": "...", "updated_at": "..."
}
```

## Errors

- `400` invalid `status`
- `404` not found
- `409` cancel race lost
- `410` gone (dead-letter)

## Curl

```bash
curl -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_type":"sleep","payload":{"duration_ms":2000},"max_attempts":3}'

curl localhost:8000/jobs/<id>
curl "localhost:8000/jobs?status=pending&limit=10" | jq
curl -X DELETE localhost:8000/jobs/<id>
curl -X POST localhost:8000/jobs/<id>/retry
curl -X POST localhost:8000/jobs/admin/archive?retention_days=7
```
