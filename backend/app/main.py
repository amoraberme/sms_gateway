from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.carrier_dlr import router as dlr_router
from app.api.dashboard import router as dashboard_router
from app.api.metrics import router as metrics_router
from app.api.v1.balance import router as balance_router
from app.api.v1.messages import router as messages_router
from app.carriers.client import CarrierClient
from app.carriers.router import CarrierRouter
from app.config import get_settings
from app.db.migrate import run_migrations
from app.db.pool import create_db_pool
from app.queue.redis_scheduler import RedisScheduler
from app.workers.dispatch_worker import dispatch_worker_loop
from app.workers.sweeper_worker import sweeper_worker_loop
from app.workers.webhook_worker import webhook_worker_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    if settings.run_migrations_on_startup:
        await run_migrations(settings.database_url)

    pool = await create_db_pool(settings.database_url)
    scheduler = RedisScheduler(settings.redis_url)
    await scheduler.init_scripts()

    app.state.db_pool = pool
    app.state.scheduler = scheduler

    stop_event = asyncio.Event()
    worker_tasks: list[asyncio.Task] = []
    carrier_client: CarrierClient | None = None

    if settings.run_embedded_workers:
        carrier_client = CarrierClient(timeout_sec=settings.carrier_timeout_sec)
        carrier_router = CarrierRouter(pool, scheduler.redis)
        for i in range(settings.dispatch_concurrency):
            worker_tasks.append(
                asyncio.create_task(
                    dispatch_worker_loop(i, pool, scheduler, carrier_router, carrier_client, stop_event)
                )
            )
        worker_tasks.append(asyncio.create_task(sweeper_worker_loop(pool, scheduler, stop_event)))
        worker_tasks.append(asyncio.create_task(webhook_worker_loop(pool, stop_event)))

    try:
        yield
    finally:
        stop_event.set()
        for t in worker_tasks:
            t.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        if carrier_client is not None:
            await carrier_client.close()
        await scheduler.close()
        await pool.close()


app = FastAPI(title="DCC Mini SMS Gateway", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(messages_router)
app.include_router(balance_router)
app.include_router(dlr_router)
app.include_router(dashboard_router)
app.include_router(metrics_router)
