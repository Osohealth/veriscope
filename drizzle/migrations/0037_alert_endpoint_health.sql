-- 0037_alert_endpoint_health.sql
-- P4.4 Step 1: alert endpoint health aggregates

CREATE TABLE IF NOT EXISTS alert_endpoint_health (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  window text NOT NULL,
  destination_type text NOT NULL,
  destination text NOT NULL,
  status text NOT NULL,
  attempts_total integer NOT NULL DEFAULT 0,
  attempts_success integer NOT NULL DEFAULT 0,
  success_rate double precision NOT NULL DEFAULT 1,
  p50_ms integer NULL,
  p95_ms integer NULL,
  consecutive_failures integer NOT NULL DEFAULT 0,
  last_success_at timestamptz NULL,
  last_failure_at timestamptz NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, window, destination_type, destination)
);

CREATE INDEX IF NOT EXISTS alert_endpoint_health_tenant_window_status
  ON alert_endpoint_health (tenant_id, window, status);

CREATE INDEX IF NOT EXISTS alert_endpoint_health_tenant_window_updated
  ON alert_endpoint_health (tenant_id, window, updated_at DESC);
