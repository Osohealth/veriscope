-- 0031_alert_noise_budgets.sql
-- P4.3.2: Per-destination noise budgets + skip reason

CREATE TABLE IF NOT EXISTS alert_noise_budgets (
  tenant_id uuid NOT NULL,
  destination_type text NOT NULL CHECK (destination_type IN ('WEBHOOK','EMAIL')),
  "window" text NOT NULL CHECK ("window" IN ('24h','7d')),
  max_deliveries integer NOT NULL CHECK (max_deliveries > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, destination_type, "window")
);

CREATE INDEX IF NOT EXISTS alert_noise_budgets_tenant_window
  ON alert_noise_budgets (tenant_id, "window");

ALTER TABLE alert_deliveries
  ADD COLUMN IF NOT EXISTS skip_reason text;

CREATE INDEX IF NOT EXISTS alert_deliveries_tenant_destination_created
  ON alert_deliveries (tenant_id, destination_type, created_at DESC);
