import asyncio
from typing import Any, Awaitable, Callable

Handler = Callable[[dict[str, Any]], Awaitable[None]]


async def echo(payload: dict[str, Any]) -> None:
    # no-op, used for correctness tests
    pass


async def sleep_handler(payload: dict[str, Any]) -> None:
    ms = int(payload.get("duration_ms", 100))
    await asyncio.sleep(ms / 1000)


async def fail_always(payload: dict[str, Any]) -> None:
    raise RuntimeError(payload.get("error", "intentional failure"))


HANDLER_REGISTRY: dict[str, Handler] = {
    "echo": echo,
    "sleep": sleep_handler,
    "fail_always": fail_always,
}


def get_handler(job_type: str) -> Handler:
    h = HANDLER_REGISTRY.get(job_type)
    if h is None:
        raise ValueError(f"unknown job_type: {job_type}")
    return h
