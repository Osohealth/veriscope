CREATE TABLE IF NOT EXISTS alert_destination_states (
  tenant_id uuid NOT NULL,
  destination_type text NOT NULL,
  destination_key text NOT NULL,
  state text NOT NULL,
  reason text NULL,
  paused_by_user_id uuid NULL,
  paused_at timestamptz NULL,
  auto_paused_at timestamptz NULL,
  resume_ready_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, destination_type, destination_key)
);

CREATE INDEX IF NOT EXISTS alert_destination_states_tenant_state
  ON alert_destination_states (tenant_id, state);

CREATE INDEX IF NOT EXISTS alert_destination_states_tenant_dest_state
  ON alert_destination_states (tenant_id, destination_type, destination_key, state);
