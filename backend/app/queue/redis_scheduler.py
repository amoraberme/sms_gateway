from __future__ import annotations

import json
import time
from typing import Any
import uuid

import redis.asyncio as aioredis

# Atomic Enqueue: deduplicate via sms:queued_set, push to per-tenant list, register in active ring
LUA_ENQUEUE = """
local queued_set = KEYS[1]
local active_set = KEYS[2]
local active_ring = KEYS[3]
local tenant_queue = KEYS[4]
local msg_id = ARGV[1]
local tenant_id = ARGV[2]

if redis.call('SADD', queued_set, msg_id) == 0 then
    return 0
end
redis.call('RPUSH', tenant_queue, msg_id)
if redis.call('SADD', active_set, tenant_id) == 1 then
    redis.call('RPUSH', active_ring, tenant_id)
end
return 1
"""

# Atomic Fair Priority Pop:
# - Checks high priority ring vs normal priority ring with 9:1 anti-starvation ratio
# - Rotates per-tenant round-robin inside the chosen tier
# - Records lease in sms:inflight ZSET
LUA_FAIR_POP = """
local high_ring = KEYS[1]
local high_set = KEYS[2]
local norm_ring = KEYS[3]
local norm_set = KEYS[4]
local prio_counter = KEYS[5]
local queued_set = KEYS[6]
local inflight_zset = KEYS[7]

local now_ms = tonumber(ARGV[1])
local lease_ttl_ms = tonumber(ARGV[2])

local function pop_from_tier(tier, ring, active_set)
    local len = redis.call('LLEN', ring)
    for _ = 1, len do
        local tenant_id = redis.call('LPOP', ring)
        if not tenant_id then
            return nil
        end
        local qkey = 'sms:q:' .. tier .. ':' .. tenant_id
        local msg_id = redis.call('LPOP', qkey)
        if msg_id then
            if redis.call('LLEN', qkey) > 0 then
                redis.call('RPUSH', ring, tenant_id)
            else
                redis.call('SREM', active_set, tenant_id)
            end
            redis.call('SREM', queued_set, msg_id)
            redis.call('ZADD', inflight_zset, now_ms + lease_ttl_ms, msg_id)
            return msg_id
        else
            redis.call('SREM', active_set, tenant_id)
        end
    end
    return nil
end

local high_len = redis.call('LLEN', high_ring)
local norm_len = redis.call('LLEN', norm_ring)

if high_len == 0 and norm_len == 0 then
    return nil
end

local chosen = nil
if high_len > 0 and norm_len > 0 then
    local c = tonumber(redis.call('INCR', prio_counter))
    if (c % 10) == 0 then
        chosen = pop_from_tier('normal', norm_ring, norm_set)
        if not chosen then
            chosen = pop_from_tier('high', high_ring, high_set)
        end
    else
        chosen = pop_from_tier('high', high_ring, high_set)
        if not chosen then
            chosen = pop_from_tier('normal', norm_ring, norm_set)
        end
    end
elseif high_len > 0 then
    chosen = pop_from_tier('high', high_ring, high_set)
else
    chosen = pop_from_tier('normal', norm_ring, norm_set)
end

return chosen
"""

# Atomic Global + Per-Carrier 1000ms Sliding Window Rate Limiter
LUA_RATE_LIMIT = """
local global_key = KEYS[1]
local carrier_key = KEYS[2]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local global_limit = tonumber(ARGV[3])
local carrier_limit = tonumber(ARGV[4])
local token = ARGV[5]

local cutoff = now_ms - window_ms
redis.call('ZREMRANGEBYSCORE', global_key, '-inf', cutoff)
redis.call('ZREMRANGEBYSCORE', carrier_key, '-inf', cutoff)

local g_count = redis.call('ZCARD', global_key)
local c_count = redis.call('ZCARD', carrier_key)

if g_count < global_limit and c_count < carrier_limit then
    redis.call('ZADD', global_key, now_ms, token)
    redis.call('ZADD', carrier_key, now_ms, token)
    redis.call('PEXPIRE', global_key, window_ms * 2)
    redis.call('PEXPIRE', carrier_key, window_ms * 2)
    return 0
end

local wait_ms = 50
if g_count >= global_limit then
    local oldest = redis.call('ZRANGE', global_key, 0, 0, 'WITHSCORES')
    if oldest and #oldest >= 2 then
        local w = tonumber(oldest[2]) + window_ms - now_ms + 2
        if w > wait_ms then wait_ms = w end
    end
end
if c_count >= carrier_limit then
    local oldest = redis.call('ZRANGE', carrier_key, 0, 0, 'WITHSCORES')
    if oldest and #oldest >= 2 then
        local w = tonumber(oldest[2]) + window_ms - now_ms + 2
        if w > wait_ms then wait_ms = w end
    end
end
return wait_ms
"""


class RedisScheduler:
    EVENTS_CHANNEL = "sms:events"

    def __init__(self, redis_url: str) -> None:
        self.redis = aioredis.from_url(redis_url, decode_responses=True)
        self._enqueue_sha: str | None = None
        self._pop_sha: str | None = None
        self._ratelimit_sha: str | None = None

    async def init_scripts(self) -> None:
        self._enqueue_sha = await self.redis.script_load(LUA_ENQUEUE)
        self._pop_sha = await self.redis.script_load(LUA_FAIR_POP)
        self._ratelimit_sha = await self.redis.script_load(LUA_RATE_LIMIT)

    async def close(self) -> None:
        await self.redis.aclose()

    async def enqueue_many(self, items: list[tuple[str, str, str]]) -> None:
        """items: list of (message_id, tenant_id, priority['high'|'normal'])"""
        if self._enqueue_sha is None:
            await self.init_scripts()
        for msg_id, tenant_id, priority in items:
            tier = "high" if priority == "high" else "normal"
            await self.redis.evalsha(
                self._enqueue_sha,
                4,
                "sms:queued_set",
                f"sms:active_set:{tier}",
                f"sms:active_ring:{tier}",
                f"sms:q:{tier}:{tenant_id}",
                msg_id,
                tenant_id,
            )

    async def pop_fair(self, lease_ttl_ms: int = 15000) -> str | None:
        if self._pop_sha is None:
            await self.init_scripts()
        now_ms = int(time.time() * 1000)
        return await self.redis.evalsha(
            self._pop_sha,
            7,
            "sms:active_ring:high",
            "sms:active_set:high",
            "sms:active_ring:normal",
            "sms:active_set:normal",
            "sms:prio_counter",
            "sms:queued_set",
            "sms:inflight",
            str(now_ms),
            str(lease_ttl_ms),
        )

    async def ack_inflight(self, message_id: str) -> None:
        await self.redis.zrem("sms:inflight", message_id)

    async def reclaim_expired_inflight(self) -> list[str]:
        now_ms = int(time.time() * 1000)
        expired = await self.redis.zrangebyscore("sms:inflight", "-inf", now_ms, start=0, num=200)
        if expired:
            await self.redis.zrem("sms:inflight", *expired)
        return list(expired)

    async def acquire_rate_limit_slot(
        self,
        carrier_id: str,
        global_limit: int,
        carrier_limit: int,
    ) -> int:
        if self._ratelimit_sha is None:
            await self.init_scripts()
        now_ms = int(time.time() * 1000)
        token = f"{now_ms}:{uuid.uuid4().hex[:8]}"
        wait_ms = await self.redis.evalsha(
            self._ratelimit_sha,
            2,
            "sms:ratelimit:global",
            f"sms:ratelimit:{carrier_id}",
            str(now_ms),
            "1000",
            str(global_limit),
            str(carrier_limit),
            token,
        )
        return int(wait_ms)

    async def queue_depths(self) -> dict[str, int]:
        high_tenants = await self.redis.smembers("sms:active_set:high")
        norm_tenants = await self.redis.smembers("sms:active_set:normal")
        high_depth = 0
        norm_depth = 0
        for tid in high_tenants:
            high_depth += int(await self.redis.llen(f"sms:q:high:{tid}"))
        for tid in norm_tenants:
            norm_depth += int(await self.redis.llen(f"sms:q:normal:{tid}"))
        return {"high": high_depth, "normal": norm_depth}

    async def publish_event(self, payload: dict[str, Any]) -> None:
        try:
            await self.redis.publish(self.EVENTS_CHANNEL, json.dumps(payload))
        except Exception:
            pass
