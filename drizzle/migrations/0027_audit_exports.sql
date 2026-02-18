CREATE TABLE IF NOT EXISTS audit_exports (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  requested_by_user_id uuid NOT NULL,
  format text NOT NULL,
  filters jsonb NOT NULL,
  row_count integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS audit_exports_tenant_created
  ON audit_exports (tenant_id, created_at DESC, id DESC);
