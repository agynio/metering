CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS usage_events (
  id UUID DEFAULT gen_random_uuid(),
  org_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL,
  producer TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  month TIMESTAMPTZ GENERATED ALWAYS AS (date_trunc('month', timestamp)) STORED,
  unit TEXT NOT NULL,
  value NUMERIC(20,6) NOT NULL,
  resource_id UUID,
  resource TEXT,
  identity_id UUID,
  identity_type TEXT,
  thread_id UUID,
  kind TEXT,
  status TEXT
) PARTITION BY RANGE (month);

CREATE UNIQUE INDEX IF NOT EXISTS usage_events_idempotency_key_month_idx
  ON usage_events (idempotency_key, month);

CREATE INDEX IF NOT EXISTS usage_events_org_timestamp_idx
  ON usage_events (org_id, timestamp);

CREATE INDEX IF NOT EXISTS usage_events_org_unit_timestamp_idx
  ON usage_events (org_id, unit, timestamp);

CREATE INDEX IF NOT EXISTS usage_events_org_identity_unit_timestamp_idx
  ON usage_events (org_id, identity_id, unit, timestamp);

CREATE INDEX IF NOT EXISTS usage_events_org_resource_unit_timestamp_idx
  ON usage_events (org_id, resource_id, unit, timestamp);
