-- Versioned schema migration 001: Core SMS Gateway, Ledger, DLR Inbox, Routing & Webhooks

CREATE TABLE IF NOT EXISTS schema_migrations (
    version    VARCHAR(64) PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$ BEGIN
    CREATE TYPE message_status AS ENUM (
        'ACCEPTED', 'QUEUED', 'SUBMITTED',
        'DELIVERED', 'UNDELIVERED', 'EXPIRED', 'FAILED'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE encoding_type AS ENUM ('GSM7', 'UCS2');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE priority_tier AS ENUM ('high', 'normal');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE ledger_entry_type AS ENUM ('DEBIT', 'REFUND');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- 1. Tenants
CREATE TABLE IF NOT EXISTS tenants (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name              VARCHAR(120) NOT NULL UNIQUE,
    slug              VARCHAR(64)  NOT NULL UNIQUE,
    api_key_hash      CHAR(64)     NOT NULL UNIQUE,
    api_key_preview   VARCHAR(64)  NOT NULL,
    default_priority  priority_tier NOT NULL DEFAULT 'normal',
    opening_balance   NUMERIC(14, 4) NOT NULL CHECK (opening_balance >= 0),
    balance           NUMERIC(14, 4) NOT NULL CHECK (balance >= 0),
    webhook_url       TEXT NULL,
    webhook_secret    VARCHAR(128) NULL,
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Destination Prefix Prices
CREATE TABLE IF NOT EXISTS prices (
    prefix            VARCHAR(16) PRIMARY KEY CHECK (prefix ~ '^\+[1-9][0-9]*$'),
    country_name      VARCHAR(80) NOT NULL,
    price_per_segment NUMERIC(14, 4) NOT NULL CHECK (price_per_segment > 0),
    currency          CHAR(3) NOT NULL DEFAULT 'USD',
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Carriers & Prefix Routing (Core + Bonus #1)
CREATE TABLE IF NOT EXISTS carriers (
    id                VARCHAR(64) PRIMARY KEY,
    name              VARCHAR(120) NOT NULL,
    base_url          TEXT NOT NULL,
    rate_limit_rps    INT NOT NULL DEFAULT 50 CHECK (rate_limit_rps > 0),
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS carrier_routes (
    id                SERIAL PRIMARY KEY,
    prefix            VARCHAR(16) NOT NULL,
    carrier_id        VARCHAR(64) NOT NULL REFERENCES carriers(id) ON DELETE CASCADE,
    priority_order    INT NOT NULL DEFAULT 1,
    UNIQUE (prefix, carrier_id)
);

-- 4. Idempotency Keys
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id         UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    idempotency_key   VARCHAR(255) NOT NULL,
    endpoint          VARCHAR(64) NOT NULL,
    request_hash      CHAR(64) NOT NULL,
    response_status   INT NOT NULL,
    response_body     JSONB NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '24 hours'),
    PRIMARY KEY (tenant_id, idempotency_key)
);

-- 5. Messages
CREATE TABLE IF NOT EXISTS messages (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         UUID NOT NULL REFERENCES tenants(id),
    client_ref        VARCHAR(255) NULL,
    to_number         VARCHAR(20) NOT NULL,
    from_sender       VARCHAR(20) NOT NULL,
    body              TEXT NOT NULL,
    encoding          encoding_type NOT NULL,
    char_units        INT NOT NULL CHECK (char_units > 0),
    segments          INT NOT NULL CHECK (segments > 0),
    price_prefix      VARCHAR(16) NOT NULL REFERENCES prices(prefix),
    unit_price        NUMERIC(14, 4) NOT NULL CHECK (unit_price > 0),
    total_cost        NUMERIC(14, 4) NOT NULL CHECK (total_cost > 0),
    priority          priority_tier NOT NULL DEFAULT 'normal',
    status            message_status NOT NULL DEFAULT 'ACCEPTED',
    status_rank       SMALLINT NOT NULL DEFAULT 10,
    carrier_used      VARCHAR(64) NULL REFERENCES carriers(id),
    carrier_id        VARCHAR(128) NULL UNIQUE,
    attempt_count     INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 4,
    last_error        TEXT NULL,
    scheduled_at      TIMESTAMPTZ NULL,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at  TIMESTAMPTZ NULL,
    accepted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    queued_at         TIMESTAMPTZ NULL,
    submitted_at      TIMESTAMPTZ NULL,
    finalized_at      TIMESTAMPTZ NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_created ON messages (tenant_id, accepted_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_status  ON messages (tenant_id, status, accepted_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_ref     ON messages (tenant_id, client_ref) WHERE client_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_sweeper        ON messages (status, next_attempt_at)
    WHERE status IN ('ACCEPTED', 'QUEUED');

-- 6. Message Event History (Immutable Audit Log)
CREATE TABLE IF NOT EXISTS message_events (
    id                BIGSERIAL PRIMARY KEY,
    message_id        UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    status            message_status NOT NULL,
    detail            TEXT NULL,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_message_events_msg ON message_events (message_id, id ASC);

-- 7. Immutable Financial Ledger
CREATE TABLE IF NOT EXISTS ledger_entries (
    id                BIGSERIAL PRIMARY KEY,
    tenant_id         UUID NOT NULL REFERENCES tenants(id),
    message_id        UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    entry_type        ledger_entry_type NOT NULL,
    amount            NUMERIC(14, 4) NOT NULL CHECK (
        (entry_type = 'DEBIT' AND amount > 0) OR
        (entry_type = 'REFUND' AND amount < 0)
    ),
    balance_after     NUMERIC(14, 4) NOT NULL CHECK (balance_after >= 0),
    description       TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (message_id, entry_type)
);
CREATE INDEX IF NOT EXISTS idx_ledger_tenant ON ledger_entries (tenant_id, id DESC);

-- 8. DLR Correlation & Deduplication Inbox
CREATE TABLE IF NOT EXISTS dlr_receipts (
    id                BIGSERIAL PRIMARY KEY,
    carrier_id        VARCHAR(128) NOT NULL,
    raw_status        VARCHAR(32) NOT NULL,
    mapped_status     message_status NOT NULL CHECK (
        mapped_status IN ('DELIVERED', 'UNDELIVERED', 'EXPIRED')
    ),
    carrier_timestamp TIMESTAMPTZ NOT NULL,
    raw_payload       JSONB NOT NULL,
    message_id        UUID NULL REFERENCES messages(id) ON DELETE SET NULL,
    processed_at      TIMESTAMPTZ NULL,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (carrier_id, raw_status)
);

-- 9. Outbound Tenant Webhook Deliveries
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         UUID NOT NULL REFERENCES tenants(id),
    message_id        UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE UNIQUE,
    target_url        TEXT NOT NULL,
    payload           JSONB NOT NULL,
    status            VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                      CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED')),
    attempt_count     INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code  INT NULL,
    last_error        TEXT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_webhooks_pending ON webhook_deliveries (status, next_attempt_at)
    WHERE status = 'PENDING';
