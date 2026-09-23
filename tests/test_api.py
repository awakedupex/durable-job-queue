import pytest


@pytest.mark.asyncio
async def test_enqueue_and_get(client):
    r = await client.post("/jobs", json={"job_type": "echo", "payload": {"x": 1}})
    assert r.status_code == 201
    data = r.json()
    jid = data["id"]
    r2 = await client.get(f"/jobs/{jid}")
    assert r2.status_code == 200
    assert r2.json()["id"] == jid


@pytest.mark.asyncio
async def test_idempotency(client):
    r1 = await client.post("/jobs", json={"job_type": "echo", "payload": {}, "idempotency_key": "idem1"})
    assert r1.status_code == 201
    r2 = await client.post("/jobs", json={"job_type": "echo", "payload": {}, "idempotency_key": "idem1"})
    # second should return same id, not error
    assert r2.status_code in (200, 201)
    assert r1.json()["id"] == r2.json()["id"]
    # only one row
    lst = await client.get("/jobs?limit=100")
    assert lst.json()["total"] == 1


@pytest.mark.asyncio
async def test_list_filter(client):
    await client.post("/jobs", json={"job_type": "echo", "payload": {}})
    await client.post("/jobs", json={"job_type": "sleep", "payload": {"duration_ms": 10}})
    r = await client.get("/jobs?job_type=echo")
    assert r.json()["total"] == 1
    assert r.json()["jobs"][0]["job_type"] == "echo"


@pytest.mark.asyncio
async def test_cancel_pending(client):
    r = await client.post("/jobs", json={"job_type": "echo", "payload": {}})
    jid = r.json()["id"]
    cr = await client.delete(f"/jobs/{jid}")
    assert cr.status_code == 200
    assert cr.json()["status"] == "cancelled"
    # cancel again -> no-op or 410
    cr2 = await client.delete(f"/jobs/{jid}")
    assert cr2.status_code in (200, 409, 410)


@pytest.mark.asyncio
async def test_cancel_leased_noop(client, pool):
    from app.queries import lease_next_job
    r = await client.post("/jobs", json={"job_type": "echo", "payload": {}})
    jid = r.json()["id"]
    leased = await lease_next_job(pool, "w1", lease_seconds=30)
    assert leased is not None
    cr = await client.delete(f"/jobs/{jid}")
    # should be no-op because leased
    assert cr.json()["status"] == "no-op"


@pytest.mark.asyncio
async def test_retry_dead_letter(client, pool):
    # enqueue max_attempts=1 so it goes to DLQ fast
    r = await client.post("/jobs", json={"job_type": "fail_always", "payload": {"error": "boom"}, "max_attempts": 1})
    jid = r.json()["id"]
    from app.queries import lease_next_job, mark_failed
    leased = await lease_next_job(pool, "w1", lease_seconds=2)
    assert leased is not None
    await mark_failed(pool, leased["id"], "boom")
    # now retry
    rr = await client.post(f"/jobs/{jid}/retry")
    assert rr.status_code == 200
    assert rr.json()["status"] == "pending"


@pytest.mark.asyncio
async def test_health_and_metrics(client):
    h = await client.get("/health")
    assert h.status_code == 200
    assert h.json()["status"] == "ok"
    m = await client.get("/metrics")
    assert m.status_code == 200
    assert b"queue_depth" in m.content
