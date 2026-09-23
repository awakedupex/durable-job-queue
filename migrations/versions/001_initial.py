"""Initial schema: job_status enum, jobs, dead_letter_jobs, indexes"""

from alembic import op

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE TYPE job_status AS ENUM ('pending', 'leased', 'succeeded', 'failed', 'dead')")
    op.execute("""
        CREATE TABLE jobs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            idempotency_key TEXT UNIQUE,
            job_type        TEXT NOT NULL,
            payload         JSONB NOT NULL,
            status          job_status NOT NULL DEFAULT 'pending',
            priority        SMALLINT NOT NULL DEFAULT 0,
            attempts        INT NOT NULL DEFAULT 0,
            max_attempts    INT NOT NULL DEFAULT 5,
            leased_by       TEXT,
            leased_until    TIMESTAMPTZ,
            run_after       TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_error      TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX idx_jobs_claimable ON jobs (priority DESC, run_after ASC)
            WHERE status = 'pending'
    """)
    op.execute("""
        CREATE INDEX idx_jobs_lease_expiry ON jobs (leased_until)
            WHERE status = 'leased'
    """)
    op.execute("""
        CREATE TABLE dead_letter_jobs (
            id              UUID PRIMARY KEY,
            job_type        TEXT NOT NULL,
            payload         JSONB NOT NULL,
            attempts        INT NOT NULL,
            last_error      TEXT,
            failed_at       TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dead_letter_jobs")
    op.execute("DROP TABLE IF EXISTS jobs")
    op.execute("DROP TYPE IF EXISTS job_status")
