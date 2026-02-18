-- 0034_alert_subscription_quality_gate.sql
-- P4.3.3.2: alert subscription quality gating + audit dedupe

ALTER TABLE alert_subscriptions
  ADD COLUMN IF NOT EXISTS min_quality_band text,
  ADD COLUMN IF NOT EXISTS min_quality_score integer;

CREATE TABLE IF NOT EXISTS alert_quality_gate_breaches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  subscription_id uuid NOT NULL,
  day date NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS alert_quality_gate_breaches_unique
  ON alert_quality_gate_breaches (tenant_id, subscription_id, day);

CREATE INDEX IF NOT EXISTS alert_quality_gate_breaches_tenant_day
  ON alert_quality_gate_breaches (tenant_id, day DESC);
