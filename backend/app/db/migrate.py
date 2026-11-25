from __future__ import annotations

import asyncio
from pathlib import Path
import asyncpg

from app.config import get_settings

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent / "migrations"


async def run_migrations(database_url: str | None = None) -> list[str]:
    dsn = database_url or get_settings().database_url
    conn = await asyncpg.connect(dsn=dsn, statement_cache_size=0, ssl=False)
    applied_now: list[str] = []
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    VARCHAR(64) PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        rows = await conn.fetch("SELECT version FROM schema_migrations")
        already_applied = {r["version"] for r in rows}

        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = sql_file.name
            if version in already_applied:
                continue
            sql = sql_file.read_text(encoding="utf-8")
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING",
                    version,
                )
            applied_now.append(version)
    finally:
        conn.terminate()
    return applied_now


if __name__ == "__main__":
    applied = asyncio.run(run_migrations())
    print("Applied migrations:", applied or "none (up to date)")
