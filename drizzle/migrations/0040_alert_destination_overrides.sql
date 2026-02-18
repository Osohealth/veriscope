CREATE TABLE IF NOT EXISTS alert_destination_overrides (
  tenant_id uuid NOT NULL,
  destination_key text NOT NULL,
  destination_type text NOT NULL,
  noise_budget_enabled boolean NOT NULL DEFAULT true,
  noise_budget_window_minutes integer NULL,
  noise_budget_max_deliveries integer NULL,
  sla_enabled boolean NOT NULL DEFAULT true,
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by_user_id uuid NULL,
  updated_by_key_id uuid NULL,
  PRIMARY KEY (tenant_id, destination_key)
);

CREATE INDEX IF NOT EXISTS alert_destination_overrides_tenant_type
  ON alert_destination_overrides (tenant_id, destination_type);

CREATE TABLE IF NOT EXISTS alert_destination_sla_overrides (
  tenant_id uuid NOT NULL,
  destination_key text NOT NULL,
  "window" text NOT NULL CHECK ("window" IN ('24h','7d')),
  p95_ms integer NULL CHECK (p95_ms IS NULL OR p95_ms > 0),
  success_rate_min_pct integer NULL CHECK (success_rate_min_pct IS NULL OR (success_rate_min_pct >= 0 AND success_rate_min_pct <= 100)),
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by_user_id uuid NULL,
  updated_by_key_id uuid NULL,
  PRIMARY KEY (tenant_id, destination_key, "window")
);

CREATE INDEX IF NOT EXISTS alert_destination_sla_overrides_tenant_window
  ON alert_destination_sla_overrides (tenant_id, "window");
