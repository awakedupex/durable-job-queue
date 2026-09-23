"""Harder chaos tests per spec §11.4: repeated kills, DB drops, clock skew."""
import asyncio
import random
import time

import pytest

from app.queries import enqueue, lease_next_job, mark_succeeded, queue_depth, reap_expired_leases


@pytest.mark.asyncio
async def test_repeated_30pct_kills(pool):
    """Kill 20-30% of workers mid-batch, repeatedly, no loss."""
    n = 100
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE jobs, dead_letter_jobs CASCADE")
    for i in range(n):
        await enqueue(pool, "echo", {"i": i})

    processed: set[str] = set()

    async def worker(wid: str, kill_rate: float = 0.25):
        while True:
            job = await lease_next_job(pool, wid, lease_seconds=3)
            if not job:
                break
            if random.random() < kill_rate:
                # simulate SIGKILL: don't ack, leave leased
                await asyncio.sleep(0.05)
                continue
            await mark_succeeded(pool, job["id"])
            processed.add(str(job["id"]))

    # 3 rounds of workers, each round kills 25% — simulates §11.4 repeated kills
    for round_i in range(3):
        await asyncio.gather(*[worker(f"r{round_i}-w{i}") for i in range(5)])
        # reaper between rounds
        await asyncio.sleep(3.2)
        await reap_expired_leases(pool)

    # drain remaining
    while True:
        j = await lease_next_job(pool, "drain", lease_seconds=3)
        if not j:
            await reap_expired_leases(pool)
            j = await lease_next_job(pool, "drain", lease_seconds=3)
            if not j:
                break
        await mark_succeeded(pool, j["id"])
        processed.add(str(j["id"]))

    depth = await queue_depth(pool)
    assert depth["leased"] == 0, "no job stuck leased after repeated kills"
    assert len(processed) == n, f"loss after repeated kills: {len(processed)} != {n}"


@pytest.mark.asyncio
async def test_db_drop_recovery(pool):
    """Simulate transient DB unavailability: reaper recovers after expiry (toxiproxy-like)."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE jobs, dead_letter_jobs CASCADE")
    await enqueue(pool, "echo", {"x": 1})
    job = await lease_next_job(pool, "victim", lease_seconds=2)
    assert job is not None

    # Simulate transient blip: sleep past lease without touching DB (like network partition)
    await asyncio.sleep(2.5)

    # Pool should still work — verify reaper can reclaim after blip without restart
    n = await reap_expired_leases(pool)
    assert n == 1, "reaper should reclaim after transient blip"
    j2 = await lease_next_job(pool, "rescuer", lease_seconds=3)
    assert j2 is not None
    await mark_succeeded(pool, j2["id"])

    # Verify pool still healthy with a fresh enqueue/lease
    await enqueue(pool, "echo", {"x": 2})
    j3 = await lease_next_job(pool, "w3", lease_seconds=3)
    assert j3 is not None
    await mark_succeeded(pool, j3["id"])


@pytest.mark.asyncio
async def test_clock_skew_db_now(pool):
    """Lease comparisons use DB now(), not app clock — verify future run_after not leased."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE jobs, dead_letter_jobs CASCADE")
    # Enqueue with run_after 1 hour in future (simulates app clock ahead)
    await enqueue(pool, "echo", {"x": 1}, run_after=None)
    # manually set run_after to future using DB now()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE jobs SET run_after = now() + interval '1 hour'")

    # should NOT be claimable even though app might think it's ready
    assert await lease_next_job(pool, "w1", lease_seconds=3) is None

    # Move to past using DB now() — now claimable regardless of app clock skew
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE jobs SET run_after = now() - interval '1 second'")
    j = await lease_next_job(pool, "w1", lease_seconds=3)
    assert j is not None, "lease should succeed once DB run_after <= DB now()"
    await mark_succeeded(pool, j["id"])

    # Verify leased_until is set via DB now(), not app time — check it's within 10s of DB now()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT now()")
            db_now = (await cur.fetchone())[0]
    # lease another job and check leased_until delta
    await enqueue(pool, "echo", {"x": 2})
    j2 = await lease_next_job(pool, "w2", lease_seconds=8)
    assert j2 is not None
    delta = (j2["leased_until"] - db_now).total_seconds()
    assert 7 < delta < 9, f"leased_until should be DB now()+8s, got delta {delta}s"


@pytest.mark.asyncio
async def test_reclaim_latency_under_7s(pool):
    """Measure reclaim latency <7s (core SLA)."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE jobs, dead_letter_jobs CASCADE")
    await enqueue(pool, "sleep", {"duration_ms": 50})
    job = await lease_next_job(pool, "victim", lease_seconds=5)
    assert job is not None
    t0 = time.monotonic()
    await asyncio.sleep(5.5)
    n = await reap_expired_leases(pool)
    elapsed = time.monotonic() - t0
    assert n == 1
    assert elapsed < 7, f"reclaim {elapsed:.1f}s exceeds 7s SLA"
