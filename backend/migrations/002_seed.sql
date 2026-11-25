-- Versioned seed migration 002: Tenants, Country Prefix Prices, Carriers, and Prefix Routes

-- Seed Tenants (Acme Bank OTP = high priority, ShopFast Bulk = normal priority)
-- API keys:
--   Acme Bank: sk_live_acme_otp_key_01
--   ShopFast : sk_live_shopfast_bulk_key_01
INSERT INTO tenants (
    id, name, slug, api_key_hash, api_key_preview, default_priority,
    opening_balance, balance, webhook_url, webhook_secret
)
VALUES
    (
        '11111111-1111-1111-1111-111111111111',
        'Acme Bank',
        'acme-bank',
        encode(sha256('sk_live_acme_otp_key_01'::bytea), 'hex'),
        'sk_live_acme...01',
        'high',
        500.0000,
        500.0000,
        NULL,
        'whsec_acme_secret_01'
    ),
    (
        '22222222-2222-2222-2222-222222222222',
        'ShopFast',
        'shopfast',
        encode(sha256('sk_live_shopfast_bulk_key_01'::bytea), 'hex'),
        'sk_live_shopfast...01',
        'normal',
        1000.0000,
        1000.0000,
        NULL,
        'whsec_shopfast_secret_01'
    )
ON CONFLICT (slug) DO NOTHING;

-- Seed Country Prefix Prices
INSERT INTO prices (prefix, country_name, price_per_segment, currency)
VALUES
    ('+880', 'Bangladesh', 0.0200, 'USD'),
    ('+971', 'United Arab Emirates', 0.0500, 'USD'),
    ('+1',   'United States / Canada', 0.0100, 'USD'),
    ('+44',  'United Kingdom', 0.0400, 'USD')
ON CONFLICT (prefix) DO NOTHING;

-- Seed Carriers
INSERT INTO carriers (id, name, base_url, rate_limit_rps, is_active)
VALUES
    ('carrier-primary', 'Mock Carrier Primary', 'http://127.0.0.1:8001', 50, TRUE),
    ('carrier-secondary', 'Mock Carrier Secondary', 'http://127.0.0.1:8002', 50, TRUE)
ON CONFLICT (id) DO NOTHING;

-- Seed Prefix Routing & Failover Priority
INSERT INTO carrier_routes (prefix, carrier_id, priority_order)
VALUES
    ('+880', 'carrier-primary',   1),
    ('+880', 'carrier-secondary', 2),
    ('+971', 'carrier-secondary', 1),
    ('+971', 'carrier-primary',   2),
    ('+',    'carrier-primary',   1),
    ('+',    'carrier-secondary', 2)
ON CONFLICT (prefix, carrier_id) DO NOTHING;
