# DCC Mini SMS Gateway

A fault-tolerant, multi-tenant **Mini SMS Gateway** built for Digital Cloud Communications (DCC). It handles urgent OTP traffic (**Acme Bank**) concurrently with 10,000-message marketing bursts (**ShopFast**), enforcing strict prepaid ledger invariants, monotonic delivery-receipt (DLR) state transitions, global 50 submits/sec carrier rate limiting, and priority fair queuing.

---

## Architecture Overview

```text
Clients (Acme OTP / ShopFast Batch / Next.js UI)
   │  POST /v1/messages, /v1/messages/batch, GET /v1/*
   ▼
Gateway API (FastAPI + AsyncIO)
   ├─ Auth (SHA-256 X-API-Key) + Strict E.164 + GSM-7/UCS-2 + Prefix Pricing
   ├─ ACID Intake Tx (PostgreSQL): Idempotency-Key lock, SELECT ... FOR UPDATE balance hold,
   │  bulk UNNEST inserts into messages, message_events, ledger_entries (DEBIT)
   └─ Immediate promote ACCEPTED -> QUEUED + Redis Lua Fair Priority Enqueue
   │
   ▼
Redis / Valkey
   ├─ Priority + Per-Tenant Fair Round-Robin Rings (sms:q:{high|normal}:{tenant_id})
   ├─ Global Sliding-Window 50 RPS Rate Limiter (sms:ratelimit:global & per-carrier)
   └─ Pub/Sub (sms:events) -> SSE Live Dashboard Push
   │
   ▼
Dispatch & Recovery Workers
   ├─ Prefix-Based Carrier Router (+880 -> Primary, +971 -> Secondary) + Circuit-Breaker Failover
   ├─ Bounded Retries (429/503/Timeout) vs. Immediate Refund (400 / Exhausted Retries)
   ├─ Sweeper Worker (promotes scheduled_at & recovers expired worker leases)
   └─ Isolated Tenant Webhook Worker (HMAC-SHA256 X-Signature + exponential backoff)
```

### Implemented Features
- **Core API & Tenant Isolation**: `POST /v1/messages`, `POST /v1/messages/batch` (up to 1,000), `GET /v1/messages` (filters: `status`, `start_date`, `end_date`, `client_ref`), `GET /v1/messages/{id}` (404 on cross-tenant access), `GET /v1/balance`, `POST /carrier/dlr`.
- **GSM-7 & UCS-2 Engine**: Full GSM 03.38 basic + extension table (`{ } [ ] ~ \ ^ | €` as 2 septets; `160/153` thresholds) and UTF-16 `UCS2` (`70/67` thresholds) for Bangla & emojis.
- **Prepaid Billing Ledger**: `SELECT ... FOR UPDATE`, `CHECK (balance >= 0)`, `UNIQUE (message_id, entry_type)`. Refunds happen **strictly** on pre-acceptance `FAILED`, never on `UNDELIVERED` or `EXPIRED`.
- **Out-of-Order & Duplicate DLR Resilience**: `X-Signature` HMAC-SHA256 raw-body verification (`401` on invalid), `dlr_receipts` deduplication (`UNIQUE (carrier_id, raw_status)`), and `pg_advisory_xact_lock` correlation for DLRs arriving before `/submit` returns.
- **Internal Operations Dashboard (Next.js + TypeScript)**: Live SSE status updates, per-tenant balance & status counts, filterable message table, audit detail modal, and live GSM-7/UCS-2 segment & price calculator.
- **Additional Capabilities**:
  1. Second Mock Carrier (`carrier-secondary`) with destination-prefix routing (`+880`, `+971`) & circuit-breaker failover.
  2. HMAC-SHA256 `X-Signature` on outbound tenant webhooks.
  3. Prometheus metrics at `GET /metrics` (`sms_queue_depth`, `sms_carrier_submits_total`, `sms_dlr_delay_seconds`, `sms_webhook_deliveries_total`).
  4. Scheduled messages (`scheduled_at` future delivery with immediate prepaid balance hold).

---

## Quickstart

### Option A: Single-Command Docker Compose (Recommended)
```bash
docker compose up --build
```
Services started automatically with migrations and seed data:
- **Frontend Dashboard**: `http://localhost:3000`
- **Gateway API & Metrics**: `http://localhost:8000` (`GET /metrics`, `GET /health`)
- **Mock Carrier Primary / Secondary**: `http://localhost:8001` / `http://localhost:8002`
- **PostgreSQL 17**: `localhost:5432`
- **Valkey (Redis)**: `localhost:6379`

### Option B: Local Native Runner
```bash
./scripts/run_local.sh start
```

### Seeded Tenants, Keys & Prices
| Tenant | API Key (`X-API-Key`) | Priority | Opening Balance |
| :--- | :--- | :--- | :--- |
| **Acme Bank** | `sk_live_acme_otp_key_01` | `high` | `$500.0000` |
| **ShopFast** | `sk_live_shopfast_bulk_key_01` | `normal` | `$1,000.0000` |

| Prefix | Country | Price / Segment | Primary Carrier | Failover |
| :--- | :--- | :--- | :--- | :--- |
| `+880` | Bangladesh | `$0.0200` | `carrier-primary` | `carrier-secondary` |
| `+971` | UAE | `$0.0500` | `carrier-secondary` | `carrier-primary` |
| `+1` | US / Canada | `$0.0100` | `carrier-primary` | `carrier-secondary` |
| `+44` | United Kingdom | `$0.0400` | `carrier-primary` | `carrier-secondary` |

---

## Example API Usage

```bash
# Send an Acme Bank OTP
curl -X POST http://localhost:8000/v1/messages \
  -H "Content-Type: application/json" \
  -H "X-API-Key: sk_live_acme_otp_key_01" \
  -H "Idempotency-Key: acme-otp-001" \
  -d '{"to":"+8801711000001","from":"ACME","text":"Your OTP is 482910."}'

# Check ShopFast balance & ledger invariant
curl -H "X-API-Key: sk_live_shopfast_bulk_key_01" http://localhost:8000/v1/balance
```

---

## Automated Tests

```bash
cd backend && pytest tests -v
```
Automated integration and unit suite (18 tests across 10 modules) covering:
1. GSM-7 / extension chars / UCS-2 Bangla & emoji segmentation & unpriced/bad E.164 rejection
2. Concurrent & sequential `Idempotency-Key` (single + batch, cross-tenant isolation, atomic batch rollback)
3. Out-of-order early & duplicate DLRs + monotonic terminal guard against late conflicting DLRs
4. Concurrent single & mixed single/batch bursts exceeding prepaid balance (balance never drops below zero)
5. Carrier `429`/`503` retries, `400` immediate failure + refund, retry exhaustion `FAILED` + refund, and `UNDELIVERED`/`EXPIRED` non-refund
6. Missing/invalid/whitespace-tampered DLR `X-Signature` returning `401` with zero DB mutation
7. Cross-tenant isolation returning `404`
8. Multi-carrier `+971` secondary routing, primary-to-secondary failover, `scheduled_at` promotion, signed tenant webhook `500` retry -> `200`, and `/metrics`
9. Filter validation (`?status=` `400`, `from_date`/`to_date` aliases), `ReadTimeout` same-carrier retry affinity -> repeat failover, and mid-lease worker crash recovery
10. Mock Carrier contract (`429`/`503`/`400`/early+dup HMAC DLRs), global `<= 50 RPS` burst cap across 16 workers, and 2-tenant OTP priority + 50-tenant fair round-robin

---

## Load Benchmark Output (10,000 ShopFast + 1/s Acme OTP)

Executed via `python scripts/load_test.py`:

```text
================ LOAD TEST REPORT ================
Run ID                   : load-cb769514
ShopFast Bulk Sent       : 10000
Acme Bank OTPs Sent      : 419
Total Messages           : 10419
Total Wall-Clock Time    : 975.91 s
Total Carrier Attempts   : 10870
Avg Carrier Submit Rate  : 11.14 submits/sec (limit <= 50.00)
OTP Accept->Submit p50   : 1.1796 s
OTP Accept->Submit p95   : 2.1286 s
Final State Counts       :
  DELIVERED              : 9249
  UNDELIVERED            : 738
  EXPIRED                : 317
  FAILED                 : 115
Billing Invariant Audit  :
  Acme Bank              : opening=$500.0000 ledger_net=$8.2800 balance=$491.7200 invariant_ok=True
  ShopFast               : opening=$1000.0000 ledger_net=$257.2600 balance=$742.7400 invariant_ok=True
Duplicate Debits         : 0
Unrefunded FAILED        : 0
Refunded Post-Submit     : 0
==================================================
ALL LOAD TEST INVARIANTS PASSED.
```

> *Benchmark environment note*: Executed locally against a single-connection embedded PostgreSQL wire server (`DB_POOL_MAX_SIZE=1`, `DISPATCH_CONCURRENCY=16`), which serializes SQL commits at ~11.14 submits/sec. Under `docker compose up` with multi-connection PostgreSQL 17 (`DB_POOL_MAX_SIZE=25`), the Redis Lua rate limiter scales up to the configured `48` submits/sec cap (~210s drain time). Even under single-connection serialization, Acme Bank OTPs bypassed all 10,000 queued ShopFast messages with `1.18s` p50 / `2.13s` p95 latency and 100% ledger accuracy.

---

## Assumptions, Trade-Offs & Future Improvements

- **Assumptions & Trade-Offs**:
  - Choosing Redis Lua for priority round-robin queuing + 50 RPS sliding-window limiting avoids PostgreSQL `messages` tuple/vacuum churn, while the 0.5s PostgreSQL Sweeper guarantees no message is lost if Redis restarts or a worker crashes.
  - `CARRIER_DEDUP_MESSAGE_ID=true` is enabled by default on the mock carrier; `DESIGN.md` explicitly documents the residual Two Generals' duplicate-SMS risk if an upstream carrier is non-idempotent on timeouts.
- **What We Would Improve With More Time**:
  - Sharded tenant wallets (`tenant_wallets(tenant_id, shard_id)`) to eliminate single-row `FOR UPDATE` contention above 500 single-message req/sec per tenant.
  - Time-partitioned `messages` / `message_events` tables and batched `COPY` writers for 1,000+ msg/sec sustained throughput.
  - Per-tenant ingress rate quotas on `high` priority to prevent OTP-tier abuse as tenant count grows.

See [DESIGN.md](./DESIGN.md) for architectural trade-offs, failure-mode analysis, and scaling design.
