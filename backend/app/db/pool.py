from __future__ import annotations

import asyncpg

from app.config import get_settings


async def create_db_pool(
    database_url: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
) -> asyncpg.Pool:
    settings = get_settings()
    dsn = database_url or settings.database_url
    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size if min_size is not None else settings.db_pool_min_size,
        max_size=max_size if max_size is not None else settings.db_pool_max_size,
        command_timeout=30,
        statement_cache_size=0,
        ssl=False,
    )
