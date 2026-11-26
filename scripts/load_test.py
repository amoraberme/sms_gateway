#!/usr/bin/env python3
"""
DCC Mini SMS Gateway Load & Fairness Benchmark:
Concurrently submits:
  1. 10,000 ShopFast bulk campaign messages (10 batches of 1,000)
  2. 1 Acme Bank urgent OTP per second while the campaign drains

Verifies:
  - Total wall-clock time & average carrier submit rate (<= 50.0 submits/sec)
  - Acme Bank OTP p50 & p95 latency from ACCEPTED -> SUBMITTED
  - Final state counts (DELIVERED, UNDELIVERED, EXPIRED, FAILED)
  - Strict billing invariants (balance == opening_balance - SUM(ledger), zero double charges)
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid

import httpx

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://127.0.0.1:8000")
ACME_KEY = os.getenv("ACME_API_KEY", "sk_live_acme_otp_key_01")
SHOPFAST_KEY = os.getenv("SHOPFAST_API_KEY", "sk_live_shopfast_bulk_key_01")

BULK_TOTAL = int(os.getenv("LOAD_BULK_TOTAL", "10000"))
BATCH_SIZE = 1000


async def send_shopfast_campaign(client: httpx.AsyncClient, run_id: str) -> int:
    accepted = 0
    num_batches = BULK_TOTAL // BATCH_SIZE
    for b in range(num_batches):
        messages = []
        for i in range(BATCH_SIZE):
            idx = b * BATCH_SIZE + i
            # 80% +880 (carrier-primary), 20% +971 (carrier-secondary)
            to_num = f"+8801711{idx:06d}" if (idx % 5) != 0 else f"+971501{idx:06d}"
            messages.append(
                {
                    "to": to_num,
                    "from": "SHOPFAST",
                    "text": f"Flash sale #{idx}: Save 25% today on ShopFast!",
                    "client_ref": f"{run_id}-bulk-{idx}",
                }
            )
        resp = await client.post(
            f"{GATEWAY_URL}/v1/messages/batch",
            headers={"X-API-Key": SHOPFAST_KEY, "Idempotency-Key": f"{run_id}-batch-{b}"},
            json={"messages": messages},
            timeout=60.0,
        )
        resp.raise_for_status()
        accepted += resp.json()["accepted_count"]
    return accepted


async def send_acme_otps(
    client: httpx.AsyncClient,
    run_id: str,
    stop_otp_event: asyncio.Event,
) -> int:
    sent = 0
    while not stop_otp_event.is_set():
        resp = await client.post(
            f"{GATEWAY_URL}/v1/messages",
            headers={"X-API-Key": ACME_KEY, "Idempotency-Key": f"{run_id}-otp-{sent}"},
            json={
                "to": f"+8801799{sent:06d}",
                "from": "ACME",
                "text": f"Your Acme Bank OTP is {100000 + (sent % 900000)}. Valid 5 min.",
                "client_ref": f"{run_id}-otp-{sent}",
            },
            timeout=60.0,
        )
        resp.raise_for_status()
        sent += 1
        try:
            await asyncio.wait_for(stop_otp_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
    return sent


async def main() -> int:
    run_id = f"load-{uuid.uuid4().hex[:8]}"
    print(f"=== Starting DCC Mini SMS Gateway Load Test (run_id={run_id}) ===")
    t_start = time.monotonic()

    async with httpx.AsyncClient() as client:
        stop_otp_event = asyncio.Event()

        bulk_task = asyncio.create_task(send_shopfast_campaign(client, run_id))
        otp_task = asyncio.create_task(send_acme_otps(client, run_id, stop_otp_event))

        bulk_accepted = await bulk_task
        print(f"[+{time.monotonic() - t_start:.1f}s] ShopFast bulk accepted: {bulk_accepted} messages")

        # Keep sending 1 OTP/sec until queued/accepted backlog drops to 0, then wait for DLRs
        while True:
            await asyncio.sleep(2.0)
            audit = (
                await client.get(
                    f"{GATEWAY_URL}/internal/dashboard/audit",
                    params={"run_prefix": run_id},
                    timeout=60.0,
                )
            ).json()
            counts = audit["status_counts"]
            in_queue = counts.get("ACCEPTED", 0) + counts.get("QUEUED", 0)
            in_flight_dlr = counts.get("SUBMITTED", 0)
            elapsed = time.monotonic() - t_start
            print(
                f"[+{elapsed:6.1f}s] total={audit['total_messages']} "
                f"queued={in_queue} submitted={in_flight_dlr} "
                f"DELIVERED={counts.get('DELIVERED', 0)} "
                f"UNDELIVERED={counts.get('UNDELIVERED', 0)} "
                f"EXPIRED={counts.get('EXPIRED', 0)} "
                f"FAILED={counts.get('FAILED', 0)}"
            )
            if in_queue == 0 and not stop_otp_event.is_set():
                stop_otp_event.set()
            if audit["non_terminal_count"] == 0 and audit["total_messages"] >= BULK_TOTAL:
                break

        otp_sent = await otp_task

    total_time = time.monotonic() - t_start
    avg_submit_rate = audit["total_carrier_attempts"] / total_time

    print("\n================ LOAD TEST REPORT ================")
    print(f"Run ID                   : {run_id}")
    print(f"ShopFast Bulk Sent       : {bulk_accepted}")
    print(f"Acme Bank OTPs Sent      : {otp_sent}")
    print(f"Total Messages           : {audit['total_messages']}")
    print(f"Total Wall-Clock Time    : {total_time:.2f} s")
    print(f"Total Carrier Attempts   : {audit['total_carrier_attempts']}")
    print(f"Avg Carrier Submit Rate  : {avg_submit_rate:.2f} submits/sec (limit <= 50.00)")
    print(f"OTP Accept->Submit p50   : {audit['otp_p50_sec']:.4f} s")
    print(f"OTP Accept->Submit p95   : {audit['otp_p95_sec']:.4f} s")
    print("Final State Counts       :")
    for st in ("DELIVERED", "UNDELIVERED", "EXPIRED", "FAILED"):
        print(f"  {st:<12}           : {audit['status_counts'].get(st, 0)}")
    print("Billing Invariant Audit  :")
    for t in audit["tenants"]:
        print(
            f"  {t['name']:<12}           : opening=${t['opening_balance']} "
            f"ledger_net=${t['ledger_net']} balance=${t['balance']} "
            f"invariant_ok={t['invariant_ok']}"
        )
    print(f"Duplicate Debits         : {audit['duplicate_debits_count']}")
    print(f"Unrefunded FAILED        : {audit['unrefunded_failed_count']}")
    print(f"Refunded Post-Submit     : {audit['refunded_post_submit_count']}")
    print("==================================================")

    assert audit["non_terminal_count"] == 0, "Messages still non-terminal"
    assert avg_submit_rate <= 50.0, f"Carrier limit exceeded: {avg_submit_rate}"
    assert audit["otp_p95_sec"] < 3.0, f"OTP p95 too high: {audit['otp_p95_sec']}"
    assert audit["duplicate_debits_count"] == 0, "Duplicate debits found!"
    assert audit["unrefunded_failed_count"] == 0, "Unrefunded FAILED found!"
    assert audit["refunded_post_submit_count"] == 0, "Post-submit message was refunded!"
    for t in audit["tenants"]:
        assert t["invariant_ok"], f"Ledger invariant failed for {t['slug']}"

    print("ALL LOAD TEST INVARIANTS PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
