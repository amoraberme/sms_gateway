# DCC Mini SMS Gateway — Architecture & Design Note

## 1. Worker Crash After Carrier Acceptance (`POST /submit` succeeds, worker dies before DB commit)

**What happens:**
When a dispatch worker pops a message from Redis, it acquires a time-bounded lease (`sms:inflight` ZSET + `lease_expires_at` in PostgreSQL, 15s TTL). If the worker crashes after receiving `200 { "carrier_id": "..." }` (or after sending the HTTP request) but before committing `status = 'SUBMITTED'` and `carrier_id` to PostgreSQL:
1. The message remains in `status = 'QUEUED'` (`status_rank = 20`) in PostgreSQL.
2. Once the 15s lease expires, the background **Recovery Sweeper** (`sweeper_worker.py`) reclaims the stale lease and re-enqueues the `message_id` into Redis.
3. Another worker picks up the message and re-submits `POST /submit` with the **exact same deterministic `message_id`**.

**Can the customer receive the SMS twice?**
It depends on the upstream carrier's `message_id` semantics:
- **Idempotent upstream carrier (`CARRIER_DEDUP_MESSAGE_ID=true`)**: The carrier deduplicates on `message_id` and returns the original `carrier_id` without sending a second physical SMS. Our worker then acquires `pg_advisory_xact_lock(hashtext('dlr:' || carrier_id))`, transitions `QUEUED -> SUBMITTED`, and immediately reconciles any DLR that may have already arrived into `dlr_receipts`.
- **Legacy non-idempotent carrier**: **Yes**, a crash in the narrow window between socket write and PostgreSQL commit (or a 5s read timeout after the carrier accepted the message) can cause a duplicate SMS delivery. This is an unavoidable consequence of the **Two Generals' Problem** across a non-transactional external HTTP boundary. We explicitly choose bounded **at-least-once dispatch** over silent message loss, while **guaranteeing zero double-charging** via `UNIQUE (message_id, entry_type)` on `ledger_entries`.

---

## 2. Where the System Is At-Least-Once vs. Exactly-Once (and Proofs)

| Boundary | Guarantee | Mechanism & Proof |
| :--- | :--- | :--- |
| **Client Send API (`POST /v1/messages`, `/batch`)** | **Exactly-Once** (with `Idempotency-Key`) | `pg_advisory_xact_lock(hashtext(tenant_id || ':idem:' || key))` + `PRIMARY KEY (tenant_id, idempotency_key)` in PostgreSQL. Same key + same SHA-256 body returns cached `202` with zero new rows; same key + different body returns `409 Conflict`. |
| **Prepaid Billing & Refunds** | **Exactly-Once** | Intake locks `SELECT balance FROM tenants WHERE id = $1 FOR UPDATE` with `CHECK (balance >= 0)`. `ledger_entries` enforces `UNIQUE (message_id, entry_type)`, physically preventing two `DEBIT`s or two `REFUND`s for any message. `REFUND` is guarded by `WHERE status_rank < 30` (pre-acceptance `FAILED` only); `UNDELIVERED`/`EXPIRED` have zero refund paths. |
| **State Transitions & DLR Processing** | **Effectively Exactly-Once** | `dlr_receipts` enforces `UNIQUE (carrier_id, raw_status)` (dropping ~10% duplicate DLRs). Early DLRs (~5%) buffer in `dlr_receipts` and correlate atomically under `pg_advisory_xact_lock('dlr:' || carrier_id)`. `messages` updates use `WHERE status_rank < :new_rank`, proving monotonic forward-only progression. |
| **Carrier HTTP `/submit`** | **At-Least-Once** (bounded `max_attempts=4`) | Across worker crashes or 5s ambiguous timeouts, the sweeper retries until `SUBMITTED` or `FAILED`. |
| **Outbound Tenant Webhooks** | **At-Least-Once** | `webhook_deliveries` has `UNIQUE (message_id)` (1 logical final-state callback per message), retried with exponential backoff up to 5 attempts and signed with `X-Signature` HMAC-SHA256. |

---

## 3. Multi-Tenant Fairness & Scaling from 2 Tenants to 50 Tenants

**How fairness works today:**
Queueing uses a two-level **Redis Lua Fair Priority Scheduler** (`app/queue/redis_scheduler.py`):
1. **Priority Tiers with Anti-Starvation**: `high` (Acme Bank OTPs) vs. `normal` (ShopFast bulk campaigns). When both tiers are non-empty, the Lua pop script dispatches up to `9` consecutive `high` messages per `1` `normal` message (`prio_counter % 10 == 0`), falling through immediately when either tier is empty. During a 10,000-message ShopFast campaign, an Acme Bank OTP enters `high` and is popped on the very next carrier rate-limit slot (~20ms).
2. **Per-Tenant Fair Round-Robin Ring**: Within each tier, messages live in per-tenant lists (`sms:q:{tier}:{tenant_id}`) indexed by an active-tenant ring (`sms:active_ring:{tier}`). Each pop takes 1 message from the head tenant and rotates that tenant to the tail of the ring if its queue remains non-empty.

**What happens with 50 tenants instead of 2:**
- Because every queue operation is $O(1)$ (`LPOP`/`RPUSH`/`SADD`), scaling from 2 to 50 tenants adds zero lock contention and negligible memory.
- If all 50 tenants backlog simultaneously in `normal`, each active tenant receives an equal `1/50` share (~1 RPS floor at 50 RPS global cap), while idle tenants consume `0` slots and active tenants elastically absorb unused capacity.
- **New risk at 50 tenants**: If multiple tenants share `high` priority and one tenant misuses `high` for bulk traffic, it could degrade OTP latency for the other 49 tenants. **Mitigation**: Add a per-tenant `high`-tier ingress token bucket (e.g., 5 OTP/sec per tenant, automatically demoting burst excess to `normal`).

---

## 4. What Breaks First at 1,000 Messages/Sec & Architectural Evolution

At **1,000 messages/sec** (20x the current 50 RPS carrier limit):
1. **Bottleneck #1 — Hot Tenant Row Lock (`SELECT ... FOR UPDATE` on `tenants`)**:
   - A single mega-tenant sending 1,000 individual `POST /v1/messages` requests/sec serializes on one `tenants` row lock (~2–3ms/tx -> ~350–500 tx/sec ceiling).
   - **Fix**: **Sharded Tenant Wallets** (`tenant_wallets(tenant_id, shard_id)` with $K=16$ sub-balances per tenant) or Redis atomic `DECRBY` credit reservation periodically settled to PostgreSQL, plus encouraging `/v1/messages/batch`.
2. **Bottleneck #2 — PostgreSQL Write & Index Amplification**:
   - 1,000 msgs/sec generates ~6,000–7,000 row inserts/updates per second (`messages`, `message_events`, `ledger_entries`, `dlr_receipts`), causing WAL and B-tree index bloat.
   - **Fix**: Range-partition `messages`, `message_events`, and `ledger_entries` by `accepted_at` (daily/weekly partitions), buffer high-volume intake/DLR writes through Kafka/Redis Streams with batched `COPY`/`UNNEST` writers (100–500 rows/tx), and scale stateless worker pods horizontally behind HTTP/2 or persistent SMPP carrier connections.
