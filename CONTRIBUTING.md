# Contributing

```bash
uv sync --group dev
uv run ruff check app tests
uv run ruff format app tests
uv run alembic upgrade head
uv run pytest -q
```

- Use `uv` and Python 3.12.
- All `now()` must be DB `now()` — never `datetime.now()` in lease/reap.
- Add tests for new queries under `tests/test_queries.py`.
- For new job types, register in `app/worker/handlers.py` and add an `echo`-like test.
- Before PR: `uv run ruff check --fix && uv run pytest -q` and include `EXPLAIN ANALYZE` if you touch `lease_next_job`.
