import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.db import get_pool, open_pool
from app.main import app


@pytest_asyncio.fixture(autouse=True, scope="function")
async def clean_db():
    pool = get_pool()
    await open_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE jobs, dead_letter_jobs CASCADE")
    yield
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE jobs, dead_letter_jobs CASCADE")


@pytest_asyncio.fixture
async def client():
    # Ensure pool opened via lifespan? open explicitly
    await open_pool()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def pool():
    p = get_pool()
    await open_pool()
    return p
