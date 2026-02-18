CREATE TABLE IF NOT EXISTS tenant_settings (
  tenant_id uuid PRIMARY KEY,
  audit_retention_days integer NOT NULL DEFAULT 90,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
