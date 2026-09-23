import json
import uuid
from datetime import datetime
from typing import Any, Optional

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


def _row_to_job(row: dict) -> dict:
    return row


# ---------- Enqueue ----------

async def enqueue(
    pool: AsyncConnectionPool,
    job_type: str,
    payload: dict[str, Any],
    idempotency_key: Optional[str] = None,
    max_attempts: int = 5,
    run_after: Optional[datetime] = None,
    priority: int = 0,
) -> dict:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # If idempotency_key provided, check existing first (handle unique violation gracefully)
            if idempotency_key is not None:
                # Try insert; on conflict return existing
                try:
                    await cur.execute(
                        """
                        INSERT INTO jobs (idempotency_key, job_type, payload, max_attempts, run_after, priority)
                        VALUES (%s, %s, %s::jsonb, %s, COALESCE(%s, now()), %s)
                        RETURNING *
                        """,
                        (idempotency_key, job_type, json.dumps(payload), max_attempts, run_after, priority),
                    )
                    row = await cur.fetchone()
                    # wake idle workers via NOTIFY (best-effort, polling is fallback if missed)
                    try:
                        await cur.execute("SELECT pg_notify('jobs_channel', %s)", (str(row["id"]),))
                    except Exception:
                        pass
                    return row
                except Exception as e:
                    # Unique violation on idempotency_key -> fetch existing
                    if "duplicate key" in str(e).lower() or "unique" in str(e).lower():
                        # Need to rollback the failed transaction? autocommit is True so no txn wrapper
                        # But psycopg raises; connection still usable with autocommit
                        await cur.execute(
                            "SELECT * FROM jobs WHERE idempotency_key = %s",
                            (idempotency_key,),
                        )
                        row = await cur.fetchone()
                        return row
                    raise
            else:
                await cur.execute(
                    """
                    INSERT INTO jobs (job_type, payload, max_attempts, run_after, priority)
                    VALUES (%s, %s::jsonb, %s, COALESCE(%s, now()), %s)
                    RETURNING *
                    """,
                    (job_type, json.dumps(payload), max_attempts, run_after, priority),
                )
                row = await cur.fetchone()
                try:
                    await cur.execute("SELECT pg_notify('jobs_channel', %s)", (str(row["id"]),))
                except Exception:
                    pass
                return row


# ---------- Lease (SKIP LOCKED) ----------

LEASE_SQL = """
WITH next_job AS (
    SELECT id FROM jobs
    WHERE status = 'pending'
      AND run_after <= now()
    ORDER BY priority DESC, run_after ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET status = 'leased',
    leased_by = %s,
    leased_until = now() + (%s::text || ' seconds')::interval,
    attempts = attempts + 1,
    updated_at = now()
FROM next_job
WHERE jobs.id = next_job.id
RETURNING jobs.*;
"""


async def lease_next_job(
    pool: AsyncConnectionPool, worker_id: str, lease_seconds: int = 8
) -> Optional[dict]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(LEASE_SQL, (worker_id, str(lease_seconds)))
            row = await cur.fetchone()
            return row


# Raw connection variant for tests that want to manage txn explicitly
async def lease_next_job_conn(
    conn: AsyncConnection, worker_id: str, lease_seconds: int = 8
) -> Optional[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(LEASE_SQL, (worker_id, str(lease_seconds)))
        row = await cur.fetchone()
        return row


# ---------- Complete ----------

async def mark_succeeded(pool: AsyncConnectionPool, job_id: uuid.UUID) -> Optional[dict]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "UPDATE jobs SET status = 'succeeded', updated_at = now() WHERE id = %s RETURNING *",
                (str(job_id),),
            )
            return await cur.fetchone()


async def mark_failed(pool: AsyncConnectionPool, job_id: uuid.UUID, error: str) -> Optional[dict]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE jobs
                SET status = (CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END)::job_status,
                    run_after = CASE WHEN attempts >= max_attempts THEN run_after
                                     ELSE now() + (interval '1 second' * power(2, attempts)) END,
                    last_error = %s,
                    leased_by = NULL,
                    leased_until = NULL,
                    updated_at = now()
                WHERE id = %s
                RETURNING *
                """,
                (error, str(job_id)),
            )
            row = await cur.fetchone()
            # If now failed and exhausted, move to dead_letter
            if row and row["status"] == "failed":
                await cur.execute(
                    """
                    INSERT INTO dead_letter_jobs (id, job_type, payload, attempts, last_error)
                    VALUES (%s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (str(row["id"]), row["job_type"], json.dumps(row["payload"]), row["attempts"], row["last_error"]),
                )
            return row


# ---------- Reaper ----------

async def reap_expired_leases(pool: AsyncConnectionPool) -> int:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE jobs
                SET status = 'pending',
                    leased_by = NULL,
                    leased_until = NULL,
                    run_after = now(),
                    updated_at = now()
                WHERE status = 'leased'
                  AND leased_until < now()
                """
            )
            return cur.rowcount


async def heartbeat_renew(pool: AsyncConnectionPool, job_id: uuid.UUID, lease_seconds: int) -> bool:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE jobs
                SET leased_until = now() + (%s::text || ' seconds')::interval,
                    updated_at = now()
                WHERE id = %s AND status = 'leased'
                """,
                (str(lease_seconds), str(job_id)),
            )
            return cur.rowcount > 0


# ---------- Read / List / Cancel ----------

async def get_job(pool: AsyncConnectionPool, job_id: uuid.UUID) -> Optional[dict]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM jobs WHERE id = %s", (str(job_id),))
            return await cur.fetchone()


async def get_dead_letter(pool: AsyncConnectionPool, job_id: uuid.UUID) -> Optional[dict]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM dead_letter_jobs WHERE id = %s", (str(job_id),))
            return await cur.fetchone()


async def list_jobs(
    pool: AsyncConnectionPool,
    status: Optional[str] = None,
    job_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            where = []
            params: list[Any] = []
            if status:
                where.append("status = %s")
                params.append(status)
            if job_type:
                where.append("job_type = %s")
                params.append(job_type)
            where_sql = ("WHERE " + " AND ".join(where)) if where else ""
            # total
            await cur.execute(f"SELECT count(*) as cnt FROM jobs {where_sql}", params)
            total = (await cur.fetchone())["cnt"]
            # page
            await cur.execute(
                f"SELECT * FROM jobs {where_sql} ORDER BY created_at DESC LIMIT %s OFFSET %s",
                params + [limit, offset],
            )
            rows = await cur.fetchall()
            return rows, total


async def cancel_job(pool: AsyncConnectionPool, job_id: uuid.UUID) -> Optional[dict]:
    """Cancel only if pending. Returns row if cancelled, None otherwise."""
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    last_error = 'cancelled',
                    updated_at = now()
                WHERE id = %s AND status = 'pending'
                RETURNING *
                """,
                (str(job_id),),
            )
            row = await cur.fetchone()
            if row:
                await cur.execute(
                    """
                    INSERT INTO dead_letter_jobs (id, job_type, payload, attempts, last_error)
                    VALUES (%s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (str(row["id"]), row["job_type"], json.dumps(row["payload"]), row["attempts"], row["last_error"]),
                )
            return row


async def retry_dead_letter(pool: AsyncConnectionPool, job_id: uuid.UUID) -> Optional[dict]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check DLQ
            await cur.execute("SELECT * FROM dead_letter_jobs WHERE id = %s", (str(job_id),))
            dl = await cur.fetchone()
            if not dl:
                # Maybe job is in jobs with status failed?
                await cur.execute("SELECT * FROM jobs WHERE id = %s AND status = 'failed'", (str(job_id),))
                row = await cur.fetchone()
                if not row:
                    return None
                # reset to pending
                await cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        run_after = now(),
                        leased_by = NULL,
                        leased_until = NULL,
                        last_error = NULL,
                        updated_at = now()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (str(job_id),),
                )
                await cur.execute("DELETE FROM dead_letter_jobs WHERE id = %s", (str(job_id),))
                return await cur.fetchone()

            # Requeue from DLQ: insert back to jobs as pending (preserve id), delete from DLQ
            await cur.execute("SELECT * FROM jobs WHERE id = %s", (str(job_id),))
            existing = await cur.fetchone()
            if existing:
                await cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        run_after = now(),
                        leased_by = NULL,
                        leased_until = NULL,
                        last_error = NULL,
                        updated_at = now()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (str(job_id),),
                )
                ret = await cur.fetchone()
            else:
                await cur.execute(
                    """
                    INSERT INTO jobs (id, job_type, payload, status, attempts, max_attempts, run_after)
                    VALUES (%s, %s, %s::jsonb, 'pending', %s, 5, now())
                    RETURNING *
                    """,
                    (str(dl["id"]), dl["job_type"], json.dumps(dl["payload"]), dl["attempts"]),
                )
                ret = await cur.fetchone()
            await cur.execute("DELETE FROM dead_letter_jobs WHERE id = %s", (str(job_id),))
            return ret


async def archive_old_jobs(pool: AsyncConnectionPool, retention_days: int = 30) -> int:
    """Move succeeded jobs older than N days to jobs_archive. Returns count moved."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT archive_old_jobs(%s)", (retention_days,))
            row = await cur.fetchone()
            return int(row[0]) if row else 0


async def queue_depth(pool: AsyncConnectionPool) -> dict[str, int]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT status, count(*) as cnt FROM jobs GROUP BY status")
            rows = await cur.fetchall()
            out = {"pending": 0, "leased": 0, "succeeded": 0, "failed": 0, "dead": 0}
            for r in rows:
                out[r["status"]] = r["cnt"]
            await cur.execute("SELECT count(*) as cnt FROM dead_letter_jobs")
            out["dead_letter"] = (await cur.fetchone())["cnt"]
            await cur.execute("SELECT count(*) as cnt FROM jobs_archive")
            out["archived"] = (await cur.fetchone())["cnt"]
            return out
