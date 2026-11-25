from __future__ import annotations

from dataclasses import dataclass
import asyncpg
import redis.asyncio as aioredis

from app.config import get_settings


@dataclass(frozen=True)
class CarrierTarget:
    id: str
    name: str
    base_url: str
    rate_limit_rps: int


class CarrierRouter:
    """Prefix-based carrier selection with Redis-backed circuit-breaker failover."""

    FAILURE_THRESHOLD = 3
    OPEN_TTL_SEC = 15

    def __init__(self, pool: asyncpg.Pool, redis_client: aioredis.Redis) -> None:
        self.pool = pool
        self.redis = redis_client
        self.settings = get_settings()

    def _effective_url(self, carrier_id: str, db_url: str) -> str:
        if carrier_id == "carrier-primary":
            return self.settings.carrier_primary_url or db_url
        if carrier_id == "carrier-secondary":
            return self.settings.carrier_secondary_url or db_url
        return db_url

    async def select_candidates(self, to_number: str) -> list[CarrierTarget]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT c.id, c.name, c.base_url, c.rate_limit_rps, r.prefix, r.priority_order
                FROM carrier_routes r
                JOIN carriers c ON c.id = r.carrier_id
                WHERE c.is_active = TRUE
                  AND $1 LIKE (r.prefix || '%')
                ORDER BY LENGTH(r.prefix) DESC, r.priority_order ASC
                """,
                to_number,
            )

        if not rows:
            return []

        best_prefix_len = len(rows[0]["prefix"])
        ordered = [
            CarrierTarget(
                id=r["id"],
                name=r["name"],
                base_url=self._effective_url(r["id"], r["base_url"]),
                rate_limit_rps=int(r["rate_limit_rps"]),
            )
            for r in rows
            if len(r["prefix"]) == best_prefix_len
        ]

        healthy: list[CarrierTarget] = []
        tripped: list[CarrierTarget] = []
        for target in ordered:
            is_open = await self.redis.exists(f"sms:cb:open:{target.id}")
            if is_open:
                tripped.append(target)
            else:
                healthy.append(target)

        return healthy + tripped

    async def record_success(self, carrier_id: str) -> None:
        await self.redis.delete(f"sms:cb:failures:{carrier_id}", f"sms:cb:open:{carrier_id}")

    async def record_failure(self, carrier_id: str) -> None:
        key = f"sms:cb:failures:{carrier_id}"
        count = await self.redis.incr(key)
        await self.redis.expire(key, 30)
        if int(count) >= self.FAILURE_THRESHOLD:
            await self.redis.set(f"sms:cb:open:{carrier_id}", "1", ex=self.OPEN_TTL_SEC)
