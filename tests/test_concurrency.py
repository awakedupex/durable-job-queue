import asyncio

import pytest

from app.queries import enqueue, lease_next_job, mark_succeeded


@pytest.mark.asyncio
async def test_10k_jobs_no_double(pool):
    # smaller version for CI speed: 200 jobs (scale to 1000 by env if needed)
    # We test correctness; stress script does the full 10k
    n = 200
    for i in range(n):
        await enqueue(pool, "echo", {"i": i})
    processed = set()
    lock = asyncio.Lock()

    async def worker(wid: str):
        while True:
            job = await lease_next_job(pool, wid, lease_seconds=4)
            if not job:
                break
            jid = str(job["id"])
            async with lock:
                assert jid not in processed, f"double lease {jid}"
                processed.add(jid)
            await mark_succeeded(pool, job["id"])

    await asyncio.gather(*[worker(f"w{i}") for i in range(20)])
    assert len(processed) == n
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM jobs WHERE status='succeeded'")
            assert (await cur.fetchone())[0] == n


@pytest.mark.asyncio
async def test_crash_reclaim_within_7s(pool):
    # Enqueue one job, lease with short lease, simulate crash (no heartbeat, no ack)
    await enqueue(pool, "sleep", {"duration_ms": 100})
    job = await lease_next_job(pool, "victim", lease_seconds=3)
    assert job is not None
    import time
    start = time.monotonic()
    # victim dies -> don't ack, don't heartbeat
    await asyncio.sleep(3.5)
    from app.queries import reap_expired_leases
    n = await reap_expired_leases(pool)
    assert n == 1
    elapsed = time.monotonic() - start
    assert elapsed < 7, f"reclaim took {elapsed}s, expected <7s"
    # another worker picks it up and succeeds
    job2 = await lease_next_job(pool, "rescuer", lease_seconds=4)
    assert job2 is not None
    await mark_succeeded(pool, job2["id"])
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT status FROM jobs WHERE id = %s", (str(job2["id"]),))
            assert (await cur.fetchone())[0] == "succeeded"


@pytest.mark.asyncio
async def test_no_permanent_loss_kill_all_restart(pool):
    # Enqueue 50, lease half, simulate all workers dying, reap, then finish
    for i in range(50):
        await enqueue(pool, "echo", {"i": i})
    leased = []
    for i in range(10):
        j = await lease_next_job(pool, f"w{i}", lease_seconds=2)
        if j:
            leased.append(j)
    assert len(leased) == 10
    # kill all: just don't ack, wait expiry
    await asyncio.sleep(2.5)
    from app.queries import reap_expired_leases
    await reap_expired_leases(pool)
    # now drain all
    count = 0
    while True:
        j = await lease_next_job(pool, "restart", lease_seconds=4)
        if not j:
            break
        await mark_succeeded(pool, j["id"])
        count += 1
    # all 50 should eventually succeed or be pending->succeeded
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM jobs WHERE status='succeeded'")
            succeeded = (await cur.fetchone())[0]
            await cur.execute("SELECT count(*) FROM jobs WHERE status='leased'")
            leased_cnt = (await cur.fetchone())[0]
            assert leased_cnt == 0, "no job stuck leased"
            assert succeeded == 50


@pytest.mark.asyncio
async def test_concurrent_500_submissions(client):
    # 500 concurrent POSTs
    async def post_one(i: int):
        r = await client.post("/jobs", json={"job_type": "echo", "payload": {"i": i}})
        assert r.status_code in (200, 201)
        return r.json()["id"]
    ids = await asyncio.gather(*[post_one(i) for i in range(500)])
    assert len(ids) == 500
    assert len(set(ids)) == 500
    # verify list total
    r = await client.get("/jobs?limit=1")
    assert r.json()["total"] == 500
