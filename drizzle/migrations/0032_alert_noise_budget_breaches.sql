-- 0032_alert_noise_budget_breaches.sql
-- P4.3.3.0: idempotent noise budget breach auditing

CREATE TABLE IF NOT EXISTS alert_noise_budget_breaches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  destination_type text NOT NULL,
  "window" text NOT NULL,
  bucket_minute timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS alert_noise_budget_breaches_unique
  ON alert_noise_budget_breaches (tenant_id, destination_type, "window", bucket_minute);

CREATE INDEX IF NOT EXISTS alert_noise_budget_breaches_tenant_bucket
  ON alert_noise_budget_breaches (tenant_id, bucket_minute DESC);
