"""Archive table + retention + declarative partitioning note + index on created_at"""

from alembic import op

revision = "002_archive"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Archive table for old succeeded jobs (keeps hot table small)
    op.execute("""
        CREATE TABLE IF NOT EXISTS jobs_archive (
            LIKE jobs INCLUDING ALL
        )
    """)
    # Index to make archiving fast
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_archive_created ON jobs_archive (created_at)
    """)
    # Index on hot table for retention queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at)
            WHERE status = 'succeeded'
    """)
    # Retention function: move succeeded jobs older than N days to archive
    op.execute("""
        CREATE OR REPLACE FUNCTION archive_old_jobs(retention_days INT)
        RETURNS INT AS $$
        DECLARE
            moved INT;
        BEGIN
            WITH moved_rows AS (
                DELETE FROM jobs
                WHERE status = 'succeeded'
                  AND created_at < now() - (retention_days || ' days')::interval
                RETURNING *
            )
            INSERT INTO jobs_archive SELECT * FROM moved_rows;
            GET DIAGNOSTICS moved = ROW_COUNT;
            RETURN moved;
        END;
        $$ LANGUAGE plpgsql;
    """)
    # Note: for true monthly declarative partitioning at scale, use:
    #   CREATE TABLE jobs_part (LIKE jobs) PARTITION BY RANGE (created_at);
    #   CREATE TABLE jobs_p2026_01 PARTITION OF jobs_part FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
    # We keep single table + archive for v1 retention, partitioning as v2 if history > few million rows.


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS archive_old_jobs(INT)")
    op.execute("DROP TABLE IF EXISTS jobs_archive")
    op.execute("DROP INDEX IF EXISTS idx_jobs_created_at")
