import asyncio
import logging

from psycopg import AsyncConnection

from app.config import settings

logger = logging.getLogger(__name__)


async def notify_listener(notify_event: asyncio.Event, stop_event: asyncio.Event):
    """
    LISTEN on jobs_channel and set notify_event when a new job arrives.
    Runs on a dedicated connection; auto-reconnects on disconnect.
    Polling remains the fallback so missed NOTIFYs are not fatal.
    """
    while not stop_event.is_set():
        conn = None
        try:
            conn = await AsyncConnection.connect(settings.database_url, autocommit=True)
            await conn.execute("LISTEN jobs_channel")
            logger.info("notify listener started (LISTEN jobs_channel)")
            gen = conn.notifies()
            while not stop_event.is_set():
                try:
                    # Wait for NOTIFY with timeout so we can check stop_event periodically
                    notify = await asyncio.wait_for(gen.__anext__(), timeout=5.0)
                    logger.debug("NOTIFY received payload=%s", notify.payload)
                    notify_event.set()
                except asyncio.TimeoutError:
                    continue
                except StopAsyncIteration:
                    break
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("notify listener error: %s (reconnecting in 2s)", e)
            await asyncio.sleep(2)
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass
        # brief backoff before reconnect if loop exits without cancel
        if not stop_event.is_set():
            await asyncio.sleep(1)
