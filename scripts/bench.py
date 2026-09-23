"""Benchmark: 10k jobs + 500 concurrent submissions, measure p50/p95/p99 and no loss."""
import asyncio
import time
import statistics

import httpx

BASE = "http://127.0.0.1:8000"


async def bench(n_jobs=10000, concurrency=500):
    latencies = []
    async with httpx.AsyncClient(timeout=30) as client:
        # health check
        r = await client.get(f"{BASE}/health")
        print("health", r.json())

        async def post_batch(start: int, count: int):
            for i in range(start, start + count):
                t0 = time.monotonic()
                resp = await client.post(f"{BASE}/jobs", json={"job_type": "echo", "payload": {"i": i}})
                t1 = time.monotonic()
                latencies.append((t1 - t0) * 1000)
                assert resp.status_code in (200, 201), resp.text

        t_start = time.monotonic()
        # fire in chunks of `concurrency` concurrent tasks
        chunk = concurrency
        for offset in range(0, n_jobs, chunk):
            batch = min(chunk, n_jobs - offset)
            await asyncio.gather(*[post_batch(offset + i, 1) for i in range(batch)])
            if offset % 1000 == 0:
                print(f" enqueued {offset+batch}/{n_jobs}")

        total = time.monotonic() - t_start
        latencies.sort()
        def pct(p):
            idx = int(len(latencies) * p / 100)
            return latencies[min(idx, len(latencies)-1)]
        print(f"\nEnqueue {n_jobs} jobs via {concurrency} concurrency: {total:.2f}s, {n_jobs/total:.1f} jobs/sec")
        print(f" p50={pct(50):.1f}ms p95={pct(95):.1f}ms p99={pct(99):.1f}ms")
        # verify no loss
        r = await client.get(f"{BASE}/jobs?limit=1")
        assert r.json()["total"] >= n_jobs
        print(" total in DB:", r.json()["total"])

if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    c = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    asyncio.run(bench(n_jobs=n, concurrency=c))
