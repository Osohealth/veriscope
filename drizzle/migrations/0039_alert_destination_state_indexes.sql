CREATE INDEX IF NOT EXISTS alert_destination_states_tenant_destination
  ON alert_destination_states (tenant_id, destination_key);

CREATE INDEX IF NOT EXISTS alert_destination_states_tenant_state_updated
  ON alert_destination_states (tenant_id, state, updated_at DESC);
