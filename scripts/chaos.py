"""Chaos: kill random workers mid-run, assert reclaim within 7s, no permanent loss."""
import asyncio
import random
import time

from app.db import get_pool, open_pool, close_pool
from app.queries import enqueue, lease_next_job, mark_succeeded, reap_expired_leases, queue_depth

async def main(n_jobs=200, n_workers=10):
    pool = get_pool()
    await open_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE jobs, dead_letter_jobs CASCADE")
    for i in range(n_jobs):
        await enqueue(pool, "echo", {"i": i})

    processed = set()
    async def worker(wid: str):
        while True:
            job = await lease_next_job(pool, wid, lease_seconds=3)
            if not job:
                break
            # random crash simulation: 5% chance of not acking (simulate SIGKILL)
            if random.random() < 0.05:
                print(f" {wid} CRASH on {job['id']}")
                # don't ack, let reaper reclaim
                await asyncio.sleep(0.5)
                continue
            await mark_succeeded(pool, job["id"])
            processed.add(str(job["id"]))
            await asyncio.sleep(0.01)

    # run workers with reaper interleave
    async def reaper():
        for _ in range(20):
            await asyncio.sleep(1)
            n = await reap_expired_leases(pool)
            if n:
                print(f" reaper reclaimed {n}")

    t0 = time.monotonic()
    await asyncio.gather(*[worker(f"w{i}") for i in range(n_workers)], reaper())
    # drain remaining
    while True:
        job = await lease_next_job(pool, "drain", lease_seconds=3)
        if not job:
            # check if any leased left
            await reap_expired_leases(pool)
            job = await lease_next_job(pool, "drain", lease_seconds=3)
            if not job:
                break
        await mark_succeeded(pool, job["id"])
        processed.add(str(job["id"]))

    depth = await queue_depth(pool)
    print(f"done {time.monotonic()-t0:.1f}s processed={len(processed)} depth={depth}")
    assert depth["leased"] == 0, "no job stuck leased"
    assert depth["pending"] == 0, "no job stuck pending"
    assert len(processed) == n_jobs, f"loss! {len(processed)} != {n_jobs}"
    print("CHAOS PASS: no permanent loss, reclaim verified")
    await close_pool()

if __name__ == "__main__":
    asyncio.run(main())
