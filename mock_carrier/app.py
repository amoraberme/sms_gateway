from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import random
import time
from typing import Literal
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel, Field, ValidationError


class SubmitRequest(BaseModel):
    message_id: str
    to: str
    from_: str = Field(alias="from")
    text: str
    encoding: Literal["GSM7", "UCS2"]

    model_config = {"populate_by_name": True}


SubmitRequest.model_rebuild()


class CarrierConfig:
    def __init__(self) -> None:
        self.carrier_name = os.getenv("CARRIER_NAME", "carrier-primary")
        self.gateway_dlr_url = os.getenv("GATEWAY_DLR_URL", "http://127.0.0.1:8000/carrier/dlr")
        self.dlr_secret = os.getenv("DLR_SECRET", "dcc-shared-dlr-hmac-secret-2026")
        self.max_rps = int(os.getenv("MAX_RPS", "50"))
        self.min_latency_ms = int(os.getenv("MIN_LATENCY_MS", "20"))
        self.max_latency_ms = int(os.getenv("MAX_LATENCY_MS", "300"))
        self.rate_503 = float(os.getenv("RATE_503", "0.03"))
        self.rate_400 = float(os.getenv("RATE_400", "0.01"))
        self.rate_timeout = float(os.getenv("RATE_TIMEOUT", "0.01"))
        self.timeout_ms = int(os.getenv("TIMEOUT_MS", "5500"))
        self.dlr_min_delay_sec = float(os.getenv("DLR_MIN_DELAY_SEC", "0.5"))
        self.dlr_max_delay_sec = float(os.getenv("DLR_MAX_DELAY_SEC", "10.0"))
        self.rate_duplicate_dlr = float(os.getenv("RATE_DUPLICATE_DLR", "0.10"))
        self.rate_early_dlr = float(os.getenv("RATE_EARLY_DLR", "0.05"))
        self.dedup_message_id = os.getenv("CARRIER_DEDUP_MESSAGE_ID", "true").lower() == "true"
        self.force_status: int | None = None


cfg = CarrierConfig()
app = FastAPI(title=f"Mock Carrier ({cfg.carrier_name})")

rps_window: deque[float] = deque()
accepted_cache: dict[str, str] = {}
background_tasks: set[asyncio.Task] = set()
http_client: httpx.AsyncClient | None = None


@app.on_event("startup")
async def on_startup() -> None:
    global http_client
    http_client = httpx.AsyncClient(timeout=10.0)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    for t in list(background_tasks):
        t.cancel()
    if http_client is not None:
        await http_client.aclose()


def _sign_body(raw_body: bytes) -> str:
    return hmac.new(cfg.dlr_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()


async def _post_dlr(carrier_id: str, dlr_status: str) -> None:
    if http_client is None:
        return
    payload = {
        "carrier_id": carrier_id,
        "status": dlr_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sig = _sign_body(raw)
    try:
        await http_client.post(
            cfg.gateway_dlr_url,
            content=raw,
            headers={"Content-Type": "application/json", "X-Signature": sig},
        )
    except Exception:
        pass


async def _send_delayed_dlr(
    carrier_id: str,
    dlr_status: str,
    delay_sec: float,
    duplicate: bool,
) -> None:
    await asyncio.sleep(delay_sec)
    await _post_dlr(carrier_id, dlr_status)
    if duplicate:
        await asyncio.sleep(0.08)
        await _post_dlr(carrier_id, dlr_status)


def _pick_dlr_status() -> str:
    r = random.random()
    if r < 0.90:
        return "DELIVRD"
    if r < 0.97:
        return "UNDELIV"
    return "EXPIRED"


def _schedule_task(coro) -> None:
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "carrier": cfg.carrier_name, "max_rps": cfg.max_rps}


@app.post("/admin/config")
async def update_config(body: dict) -> dict:
    for k, v in body.items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)
    return {"status": "updated", "force_status": cfg.force_status, "rate_503": cfg.rate_503}


@app.post("/submit")
async def submit_sms(request: Request) -> JSONResponse:
    payload = await request.json()
    try:
        parsed = SubmitRequest.model_validate(payload)
    except ValidationError as exc:
        return JSONResponse(status_code=400, content={"error": "INVALID_SUBMIT_PAYLOAD", "detail": str(exc)})
    message_id = parsed.message_id

    # Enforce strict global sliding-window RPS cap (>50/sec -> 429)
    now = time.monotonic()
    while rps_window and now - rps_window[0] >= 1.0:
        rps_window.popleft()
    if len(rps_window) >= cfg.max_rps:
        return JSONResponse(status_code=429, content={"error": "TOO_MANY_REQUESTS"})
    rps_window.append(now)

    # Optional test override
    if cfg.force_status == 400:
        return JSONResponse(status_code=400, content={"error": "INVALID_DESTINATION"})
    if cfg.force_status == 503:
        return JSONResponse(status_code=503, content={"error": "TEMPORARY_FAILURE"})
    if cfg.force_status == 429:
        return JSONResponse(status_code=429, content={"error": "TOO_MANY_REQUESTS"})

    # Deduplicate by message_id if enabled
    if cfg.dedup_message_id and message_id in accepted_cache:
        await asyncio.sleep(random.uniform(cfg.min_latency_ms, cfg.max_latency_ms) / 1000.0)
        return JSONResponse(status_code=200, content={"carrier_id": accepted_cache[message_id]})

    roll = random.random()
    if roll < cfg.rate_400:
        await asyncio.sleep(random.uniform(cfg.min_latency_ms, cfg.max_latency_ms) / 1000.0)
        return JSONResponse(status_code=400, content={"error": "INVALID_DESTINATION"})

    if roll < cfg.rate_400 + cfg.rate_503:
        await asyncio.sleep(random.uniform(cfg.min_latency_ms, cfg.max_latency_ms) / 1000.0)
        return JSONResponse(status_code=503, content={"error": "TEMPORARY_FAILURE"})

    carrier_id = f"c-{uuid.uuid4().hex[:12]}"
    accepted_cache[message_id] = carrier_id
    dlr_status = _pick_dlr_status()
    is_dup = random.random() < cfg.rate_duplicate_dlr

    # Ambiguous timeout (~1%): carrier accepts & delivers, but response takes >5s
    if roll < cfg.rate_400 + cfg.rate_503 + cfg.rate_timeout:
        delay = random.uniform(cfg.dlr_min_delay_sec, cfg.dlr_max_delay_sec)
        _schedule_task(_send_delayed_dlr(carrier_id, dlr_status, delay, is_dup))
        await asyncio.sleep(cfg.timeout_ms / 1000.0)
        return JSONResponse(status_code=200, content={"carrier_id": carrier_id})

    # Early DLR (~5%): DLR arrives before /submit HTTP response returns
    if random.random() < cfg.rate_early_dlr:
        await _post_dlr(carrier_id, dlr_status)
        if is_dup:
            await _post_dlr(carrier_id, dlr_status)
        await asyncio.sleep(0.03)
        return JSONResponse(status_code=200, content={"carrier_id": carrier_id})

    # Normal accept: 20-300ms latency, DLR in 0.5-10s
    await asyncio.sleep(random.uniform(cfg.min_latency_ms, cfg.max_latency_ms) / 1000.0)
    delay = random.uniform(cfg.dlr_min_delay_sec, cfg.dlr_max_delay_sec)
    _schedule_task(_send_delayed_dlr(carrier_id, dlr_status, delay, is_dup))
    return JSONResponse(status_code=200, content={"carrier_id": carrier_id})
