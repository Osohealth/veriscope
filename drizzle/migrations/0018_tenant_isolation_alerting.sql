-- Add tenant_id to alerting tables and api_keys for tenant isolation
-- Sentinel tenant for MVP

ALTER TABLE alert_subscriptions
  ADD COLUMN IF NOT EXISTS tenant_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';

ALTER TABLE alert_deliveries
  ADD COLUMN IF NOT EXISTS tenant_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';

ALTER TABLE alert_delivery_attempts
  ADD COLUMN IF NOT EXISTS tenant_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';

ALTER TABLE alert_runs
  ADD COLUMN IF NOT EXISTS tenant_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';

ALTER TABLE alert_dlq
  ADD COLUMN IF NOT EXISTS tenant_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';

ALTER TABLE alert_dedupe
  ADD COLUMN IF NOT EXISTS tenant_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';

ALTER TABLE api_keys
  ADD COLUMN IF NOT EXISTS tenant_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';

UPDATE alert_subscriptions
  SET tenant_id = '00000000-0000-0000-0000-000000000001'
  WHERE tenant_id IS NULL;

UPDATE alert_deliveries
  SET tenant_id = '00000000-0000-0000-0000-000000000001'
  WHERE tenant_id IS NULL;

UPDATE alert_delivery_attempts
  SET tenant_id = '00000000-0000-0000-0000-000000000001'
  WHERE tenant_id IS NULL;

UPDATE alert_runs
  SET tenant_id = '00000000-0000-0000-0000-000000000001'
  WHERE tenant_id IS NULL;

UPDATE alert_dlq
  SET tenant_id = '00000000-0000-0000-0000-000000000001'
  WHERE tenant_id IS NULL;

UPDATE alert_dedupe
  SET tenant_id = '00000000-0000-0000-0000-000000000001'
  WHERE tenant_id IS NULL;

UPDATE api_keys
  SET tenant_id = '00000000-0000-0000-0000-000000000001'
  WHERE tenant_id IS NULL;

DROP INDEX IF EXISTS idx_alert_subs_user_enabled;
CREATE INDEX IF NOT EXISTS idx_alert_subs_tenant_user_enabled
  ON alert_subscriptions (tenant_id, user_id, is_enabled);

DROP INDEX IF EXISTS idx_alert_subs_entity;
CREATE INDEX IF NOT EXISTS idx_alert_subs_tenant_entity
  ON alert_subscriptions (tenant_id, entity_type, entity_id);

DROP INDEX IF EXISTS alert_subscriptions_unique;
CREATE UNIQUE INDEX IF NOT EXISTS alert_subscriptions_unique
  ON alert_subscriptions (tenant_id, user_id, channel, endpoint, entity_id);

DROP INDEX IF EXISTS alert_dedupe_unique;
CREATE UNIQUE INDEX IF NOT EXISTS alert_dedupe_unique
  ON alert_dedupe (tenant_id, cluster_id, channel, endpoint);

DROP INDEX IF EXISTS alert_deliveries_user_created_id;
CREATE INDEX IF NOT EXISTS alert_deliveries_tenant_user_created_id
  ON alert_deliveries (tenant_id, user_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS alert_delivery_attempts_tenant_delivery_id
  ON alert_delivery_attempts (tenant_id, delivery_id);

CREATE INDEX IF NOT EXISTS idx_alert_runs_tenant_started_at
  ON alert_runs (tenant_id, started_at);

CREATE INDEX IF NOT EXISTS alert_dlq_tenant_next_attempt
  ON alert_dlq (tenant_id, next_attempt_at);

CREATE INDEX IF NOT EXISTS alert_dlq_tenant_attempt_count
  ON alert_dlq (tenant_id, attempt_count);
