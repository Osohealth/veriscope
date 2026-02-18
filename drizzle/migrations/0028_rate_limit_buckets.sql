-- 0028_rate_limit_buckets.sql
CREATE TABLE IF NOT EXISTS rate_limit_buckets (
  tenant_id uuid NOT NULL,
  key_hash text NOT NULL,
  scope text NOT NULL,
  window_start timestamptz NOT NULL,
  count integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, key_hash, scope, window_start)
);

CREATE INDEX IF NOT EXISTS rate_limit_buckets_tenant_scope_window
  ON rate_limit_buckets (tenant_id, scope, window_start DESC);
