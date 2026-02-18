-- Aggregated SLA windows for alert delivery reliability

CREATE TABLE IF NOT EXISTS alert_delivery_sla_windows (
  tenant_id uuid NOT NULL,
  destination_type text NOT NULL,
  window text NOT NULL,
  window_start timestamptz NOT NULL,

  attempts_total integer NOT NULL,
  attempts_success integer NOT NULL,
  attempts_failed integer NOT NULL,

  latency_p50_ms integer NOT NULL,
  latency_p95_ms integer NOT NULL,

  success_rate numeric(5,2) NOT NULL,
  status text NOT NULL,

  computed_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (tenant_id, destination_type, window, window_start)
);

CREATE INDEX IF NOT EXISTS alert_delivery_sla_windows_tenant_window
  ON alert_delivery_sla_windows (tenant_id, window, window_start DESC);
