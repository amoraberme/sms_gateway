from __future__ import annotations

import asyncio
import signal

from app.carriers.client import CarrierClient
from app.carriers.router import CarrierRouter
from app.config import get_settings
from app.db.pool import create_db_pool
from app.queue.redis_scheduler import RedisScheduler
from app.workers.dispatch_worker import dispatch_worker_loop
from app.workers.sweeper_worker import sweeper_worker_loop
from app.workers.webhook_worker import webhook_worker_loop


async def main() -> None:
    settings = get_settings()
    pool = await create_db_pool(settings.database_url)
    scheduler = RedisScheduler(settings.redis_url)
    await scheduler.init_scripts()
    carrier_client = CarrierClient(timeout_sec=settings.carrier_timeout_sec)
    carrier_router = CarrierRouter(pool, scheduler.redis)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    tasks: list[asyncio.Task] = []
    for i in range(settings.dispatch_concurrency):
        tasks.append(
            asyncio.create_task(
                dispatch_worker_loop(i, pool, scheduler, carrier_router, carrier_client, stop_event)
            )
        )
    tasks.append(asyncio.create_task(sweeper_worker_loop(pool, scheduler, stop_event)))
    tasks.append(asyncio.create_task(webhook_worker_loop(pool, stop_event)))

    await stop_event.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await carrier_client.close()
    await scheduler.close()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
