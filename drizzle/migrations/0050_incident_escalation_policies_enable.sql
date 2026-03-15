ALTER TABLE incident_escalation_policies
  ADD COLUMN IF NOT EXISTS last_validated_at timestamptz,
  ADD COLUMN IF NOT EXISTS last_routing_health jsonb;

CREATE INDEX IF NOT EXISTS incident_escalation_policies_enabled_idx
  ON incident_escalation_policies (tenant_id, enabled, incident_type, level);
