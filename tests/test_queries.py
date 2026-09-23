import asyncio

import pytest

from app.queries import (
    enqueue,
    heartbeat_renew,
    lease_next_job,
    mark_succeeded,
    reap_expired_leases,
)


@pytest.mark.asyncio
async def test_lease_atomicity_no_double_process(pool):
    # enqueue 20 jobs
    for i in range(20):
        await enqueue(pool, "echo", {"i": i})
    # 20 workers race
    results = await asyncio.gather(*[lease_next_job(pool, f"w{i}", lease_seconds=8) for i in range(20)])
    ids = [r["id"] for r in results if r]
    assert len(ids) == 20
    assert len(set(ids)) == 20  # no duplicates (SKIP LOCKED)
    # next lease should be None
    extra = await lease_next_job(pool, "w_extra", lease_seconds=8)
    assert extra is None
    # mark all succeeded
    for r in results:
        if r:
            await mark_succeeded(pool, r["id"])
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM jobs WHERE status='succeeded'")
            assert (await cur.fetchone())[0] == 20


@pytest.mark.asyncio
async def test_reap_reclaim(pool):
    await enqueue(pool, "echo", {"x": 1})
    job = await lease_next_job(pool, "w1", lease_seconds=2)
    assert job is not None
    # before expiry
    assert await reap_expired_leases(pool) == 0
    await asyncio.sleep(2.5)
    assert await reap_expired_leases(pool) == 1
    # can lease again
    job2 = await lease_next_job(pool, "w2", lease_seconds=2)
    assert job2 is not None
    assert str(job2["id"]) == str(job["id"])


@pytest.mark.asyncio
async def test_heartbeat_prevents_reap(pool):
    await enqueue(pool, "echo", {"x": 1})
    job = await lease_next_job(pool, "w1", lease_seconds=4)
    assert job is not None
    # heartbeat after 1s extends lease
    await asyncio.sleep(1)
    ok = await heartbeat_renew(pool, job["id"], 4)
    assert ok
    await asyncio.sleep(2)
    # should NOT be reaped because renewed
    assert await reap_expired_leases(pool) == 0
    # after another 3s it should expire
    await asyncio.sleep(3)
    assert await reap_expired_leases(pool) == 1


@pytest.mark.asyncio
async def test_backoff_and_dead_letter(pool):
    j = await enqueue(pool, "fail_always", {}, max_attempts=2)
    jid = j["id"]
    from app.queries import mark_failed

    # attempt 1
    leased = await lease_next_job(pool, "w1", lease_seconds=2)
    assert leased["attempts"] == 1
    row = await mark_failed(pool, leased["id"], "err")
    assert row["status"] == "pending"
    # should not be claimable immediately (backoff ~2s)
    assert await lease_next_job(pool, "w2", lease_seconds=2) is None
    # force run_after to now for test speed
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE jobs SET run_after = now() WHERE id = %s", (str(jid),))
    leased2 = await lease_next_job(pool, "w2", lease_seconds=2)
    assert leased2["attempts"] == 2
    row2 = await mark_failed(pool, leased2["id"], "err2")
    assert row2["status"] == "failed"
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM dead_letter_jobs WHERE id = %s", (str(jid),))
            assert (await cur.fetchone())[0] == 1
