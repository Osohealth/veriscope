-- 0047_incident_escalation_targets.sql
-- Extend escalation policies to support multiple targets per level.

ALTER TABLE incident_escalation_policies
  ADD COLUMN IF NOT EXISTS target_type text NOT NULL DEFAULT 'ROLE',
  ADD COLUMN IF NOT EXISTS target_ref text NOT NULL DEFAULT 'OWNER',
  ADD COLUMN IF NOT EXISTS target_name text;

-- Replace unique index to allow multiple targets per level.
DROP INDEX IF EXISTS incident_escalation_policies_unique;

CREATE UNIQUE INDEX IF NOT EXISTS incident_escalation_policies_unique
  ON incident_escalation_policies (tenant_id, incident_type, severity_min, level, target_type, target_ref);
