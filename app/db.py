from psycopg_pool import AsyncConnectionPool

from app.config import settings

_pool: AsyncConnectionPool | None = None


def get_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo=settings.database_url,
            min_size=2,
            max_size=settings.max_pool_size,
            open=False,
            kwargs={"autocommit": True},
        )
    return _pool


async def open_pool():
    pool = get_pool()
    if not pool._opened:
        await pool.open()


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
