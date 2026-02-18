CREATE TABLE IF NOT EXISTS audit_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  actor_type text NOT NULL,
  actor_user_id uuid,
  actor_api_key_id uuid,
  actor_label text,
  action text NOT NULL,
  resource_type text NOT NULL,
  resource_id uuid,
  severity text NOT NULL,
  status text NOT NULL,
  message text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  ip text,
  user_agent text,
  request_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS audit_events_tenant_created_id
  ON audit_events (tenant_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS audit_events_tenant_action_created
  ON audit_events (tenant_id, action, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS audit_events_tenant_resource_created
  ON audit_events (tenant_id, resource_type, resource_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS audit_events_tenant_actor_created
  ON audit_events (tenant_id, actor_type, actor_user_id, actor_api_key_id, created_at DESC, id DESC);
