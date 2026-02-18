-- 0030_alert_sla_thresholds.sql
-- P4.3.1 per-destination SLA thresholds

CREATE TABLE IF NOT EXISTS alert_sla_thresholds (
  tenant_id uuid NOT NULL,
  "window" text NOT NULL CHECK ("window" IN ('24h','7d')),
  destination_type text NOT NULL CHECK (destination_type IN ('WEBHOOK','EMAIL')),
  p95_ms_threshold integer NOT NULL CHECK (p95_ms_threshold > 0),
  success_rate_threshold numeric NOT NULL CHECK (success_rate_threshold >= 0 AND success_rate_threshold <= 1),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, "window", destination_type)
);

CREATE INDEX IF NOT EXISTS alert_sla_thresholds_tenant_window
  ON alert_sla_thresholds (tenant_id, "window");
