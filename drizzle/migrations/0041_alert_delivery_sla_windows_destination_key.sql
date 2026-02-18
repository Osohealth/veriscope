ALTER TABLE alert_delivery_sla_windows
  ADD COLUMN IF NOT EXISTS destination_key text;

UPDATE alert_delivery_sla_windows
SET destination_key = '__ALL__'
WHERE destination_key IS NULL;

ALTER TABLE alert_delivery_sla_windows
  ALTER COLUMN destination_key SET NOT NULL;

ALTER TABLE alert_delivery_sla_windows
  DROP CONSTRAINT IF EXISTS alert_delivery_sla_windows_pkey;

ALTER TABLE alert_delivery_sla_windows
  ADD CONSTRAINT alert_delivery_sla_windows_pkey
  PRIMARY KEY (tenant_id, "window", destination_type, destination_key, window_start);

CREATE INDEX IF NOT EXISTS alert_sla_windows_tenant_window_destkey_updated
  ON alert_delivery_sla_windows (tenant_id, "window", destination_type, destination_key, computed_at DESC);

CREATE INDEX IF NOT EXISTS alert_sla_windows_tenant_window_updated
  ON alert_delivery_sla_windows (tenant_id, "window", computed_at DESC);
