-- ═══════════════════════════════════════════════════════
-- LAB NUTRITION Subscriptions — Supabase Schema v1.0
-- Run this in Supabase SQL Editor
-- ═══════════════════════════════════════════════════════

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── SUBSCRIPTIONS ────────────────────────────────────────
CREATE TABLE subscriptions (
  id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  
  -- Customer (Shopify)
  customer_id         TEXT NOT NULL,
  customer_email      TEXT NOT NULL,
  customer_name       TEXT,
  customer_phone      TEXT,
  
  -- Product
  variant_id          TEXT NOT NULL,
  product_id          TEXT NOT NULL,
  product_title       TEXT NOT NULL,
  product_image       TEXT,
  
  -- Plan
  frequency_months    INTEGER NOT NULL CHECK (frequency_months IN (1, 2)),
  permanence_months   INTEGER NOT NULL CHECK (permanence_months IN (3, 6, 12)),
  discount_pct        INTEGER NOT NULL CHECK (discount_pct BETWEEN 0 AND 100),
  base_price          NUMERIC(10,2) NOT NULL,
  final_price         NUMERIC(10,2) NOT NULL,
  currency            TEXT NOT NULL DEFAULT 'PEN',
  
  -- Mercado Pago
  mp_preapproval_id   TEXT UNIQUE,
  mp_plan_id          TEXT,
  mp_payer_id         TEXT,
  mp_card_token       TEXT,
  
  -- Lifecycle
  status              TEXT NOT NULL DEFAULT 'active' 
                        CHECK (status IN ('pending','active','paused','cancelled','expired','payment_failed')),
  cycles_completed    INTEGER NOT NULL DEFAULT 0,
  cycles_required     INTEGER NOT NULL,   -- = permanence / frequency
  
  -- Dates
  started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  next_charge_at      TIMESTAMPTZ,
  paused_until        TIMESTAMPTZ,
  cancelled_at        TIMESTAMPTZ,
  expires_at          TIMESTAMPTZ,
  
  -- Shopify addresses
  shipping_address    JSONB,
  
  -- Gifts awarded
  gifts_awarded       JSONB DEFAULT '[]',
  
  -- Metadata
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── SUBSCRIPTION EVENTS (audit log) ─────────────────────
CREATE TABLE subscription_events (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  subscription_id UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
  event_type      TEXT NOT NULL,  -- 'charge_success','charge_failed','paused','skipped','cancelled','gift_sent','resumed'
  amount          NUMERIC(10,2),
  mp_payment_id   TEXT,
  shopify_order_id TEXT,
  metadata        JSONB DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── SUBSCRIPTION PLANS (no-code config) ──────────────────
CREATE TABLE subscription_plans (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  frequency_months  INTEGER NOT NULL,
  permanence_months INTEGER NOT NULL,
  discount_pct      INTEGER NOT NULL,
  extra_points      BOOLEAN NOT NULL DEFAULT false,
  gift_cycle        INTEGER,          -- which cycle number triggers gift
  gift_product_id   TEXT,             -- Shopify product/variant to send as gift
  gift_description  TEXT,
  is_active         BOOLEAN NOT NULL DEFAULT true,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(frequency_months, permanence_months)
);

-- Initial benefit matrix
INSERT INTO subscription_plans (frequency_months, permanence_months, discount_pct, extra_points, gift_cycle, gift_description) VALUES
  (1, 3,  25, false, NULL, NULL),
  (2, 3,  20, false, NULL, NULL),
  (1, 6,  30, true,  NULL, NULL),
  (2, 6,  30, false, NULL, NULL),
  (1, 12, 30, true,  12, 'CREATINE Micronized 300g GRATIS'),
  (2, 12, 30, false,  6, 'Shaker LAB NUTRITION Premium GRATIS');

-- ── ELIGIBLE PRODUCTS (no-code activation) ──────────────
CREATE TABLE eligible_products (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  shopify_product_id TEXT UNIQUE NOT NULL,
  product_title     TEXT,
  is_active         BOOLEAN NOT NULL DEFAULT true,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── INDEXES ──────────────────────────────────────────────
CREATE INDEX idx_subs_customer ON subscriptions(customer_id);
CREATE INDEX idx_subs_status ON subscriptions(status);
CREATE INDEX idx_subs_next_charge ON subscriptions(next_charge_at) WHERE status = 'active';
CREATE INDEX idx_events_sub ON subscription_events(subscription_id);

-- ── UPDATED_AT trigger ───────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_subs_updated_at BEFORE UPDATE ON subscriptions
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ── ROW LEVEL SECURITY (RLS) ─────────────────────────────
ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE eligible_products ENABLE ROW LEVEL SECURITY;

-- Service role has full access (backend uses service key)
-- Public has no direct access (all goes through backend API)

CREATE TABLE IF NOT EXISTS subscription_stacks (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), name TEXT NOT NULL, description TEXT, goal TEXT, products JSONB DEFAULT '[]', discount_pct INTEGER DEFAULT 25, is_active BOOLEAN DEFAULT true, created_at TIMESTAMPTZ DEFAULT NOW());
CREATE TABLE IF NOT EXISTS app_settings (id INTEGER PRIMARY KEY DEFAULT 1, cancellation_window_open INTEGER DEFAULT 30, cancellation_window_close INTEGER DEFAULT 15, max_pause_months INTEGER DEFAULT 2, widget_enabled BOOLEAN DEFAULT true, discount_badge_text TEXT DEFAULT 'HASTA -30%', brand_color TEXT DEFAULT '#9d2a23', updated_at TIMESTAMPTZ DEFAULT NOW());
INSERT INTO app_settings DEFAULT VALUES ON CONFLICT(id) DO NOTHING;
