CREATE TABLE IF NOT EXISTS incidents (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id uuid NOT NULL,

  type text NOT NULL,
  destination_key text NULL,

  status text NOT NULL DEFAULT 'OPEN',
  severity text NOT NULL,

  title text NOT NULL,
  summary text NOT NULL,

  opened_at timestamptz NOT NULL DEFAULT now(),
  acked_at timestamptz NULL,
  resolved_at timestamptz NULL,

  opened_by_actor_type text NOT NULL,
  opened_by_actor_id uuid NULL,

  acked_by_actor_type text NULL,
  acked_by_actor_id uuid NULL,

  resolved_by_actor_type text NULL,
  resolved_by_actor_id uuid NULL
);

CREATE INDEX IF NOT EXISTS incidents_tenant_status_opened
  ON incidents (tenant_id, status, opened_at DESC);

CREATE INDEX IF NOT EXISTS incidents_tenant_destination
  ON incidents (tenant_id, destination_key, opened_at DESC);
