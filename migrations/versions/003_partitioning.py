"""
Declarative range partitioning for jobs_archive (and example for jobs).

For production at > few million rows, partition jobs by created_at monthly:
  jobs is partitioned, each month is a child table. Queries with
  WHERE status='pending' prune to hot partition(s) via constraint exclusion.
  Archive uses the same scheme.

This migration:
- Converts jobs_archive to partitioned by RANGE (created_at) with monthly partitions
- Keeps jobs as single table for v1 (easier local dev), but creates the
  helper function ensure_month_partition() so adding partitioning to jobs is one ALTER.
- Documents the production partitioning DDL in comments.
"""
from alembic import op

revision = "003_partitioning"
down_revision = "002_archive"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Helper to auto-create monthly partitions for an arbitrary table
    op.execute("""
        CREATE OR REPLACE FUNCTION ensure_month_partition(table_name TEXT, month DATE)
        RETURNS VOID AS $$
        DECLARE
            part_name TEXT;
            start_date DATE := date_trunc('month', month)::date;
            end_date DATE := (date_trunc('month', month) + interval '1 month')::date;
        BEGIN
            part_name := table_name || '_p' || to_char(start_date, 'YYYY_MM');
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                part_name, table_name, start_date, end_date
            );
        END;
        $$ LANGUAGE plpgsql;
    """)

    # Rebuild jobs_archive as partitioned. Partition key must be in PK, so PK is (id, created_at).
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class WHERE relkind='p' AND relname='jobs_archive'
            ) THEN
                IF EXISTS (SELECT 1 FROM pg_class WHERE relname='jobs_archive' AND relkind='r') THEN
                    ALTER TABLE jobs_archive RENAME TO jobs_archive_old;
                END IF;

                CREATE TABLE jobs_archive (
                    id UUID NOT NULL,
                    idempotency_key TEXT,
                    job_type TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    status job_status NOT NULL DEFAULT 'pending',
                    priority SMALLINT NOT NULL DEFAULT 0,
                    attempts INT NOT NULL DEFAULT 0,
                    max_attempts INT NOT NULL DEFAULT 5,
                    leased_by TEXT,
                    leased_until TIMESTAMPTZ,
                    run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (id, created_at),
                    UNIQUE (idempotency_key, created_at)
                ) PARTITION BY RANGE (created_at);

                CREATE TABLE jobs_archive_p2026_07 PARTITION OF jobs_archive FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
                CREATE TABLE jobs_archive_p2026_08 PARTITION OF jobs_archive FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
                CREATE TABLE jobs_archive_p2026_09 PARTITION OF jobs_archive FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
                CREATE TABLE jobs_archive_p2026_10 PARTITION OF jobs_archive FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
                CREATE TABLE jobs_archive_default PARTITION OF jobs_archive DEFAULT;

                CREATE INDEX idx_jobs_archive_created_pt ON jobs_archive (created_at);
                CREATE INDEX idx_jobs_archive_status ON jobs_archive (status);
                CREATE INDEX idx_jobs_archive_claimable_pt ON jobs_archive (priority DESC, run_after) WHERE status = 'pending';

                INSERT INTO jobs_archive SELECT * FROM jobs_archive_old ON CONFLICT DO NOTHING;
                DROP TABLE jobs_archive_old;
            END IF;
        END $$;
    """)

    # Example DDL for converting jobs itself to partitioned (kept as comment for prod cutover):
    op.execute("""
        COMMENT ON TABLE jobs IS 'To partition jobs in production: '
            'CREATE TABLE jobs_new (LIKE jobs INCLUDING ALL) PARTITION BY RANGE (created_at); '
            'CREATE TABLE jobs_p2026_09 PARTITION OF jobs_new FOR VALUES FROM (''2026-09-01'') TO (''2026-10-01''); '
            'INSERT INTO jobs_new SELECT * FROM jobs; '
            'ALTER TABLE jobs RENAME TO jobs_old; ALTER TABLE jobs_new RENAME TO jobs; '
            'Keep idx_jobs_claimable as local index on parent; HOT path will prune to current month partition.'
    """)


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS ensure_month_partition(TEXT, DATE)")
    # downgrade keeps archive as partitioned; manual revert: detach partitions and recreate plain table
    op.execute("COMMENT ON TABLE jobs IS NULL")
