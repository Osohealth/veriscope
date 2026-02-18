-- 0046_incident_escalations.sql
-- Requires pgcrypto for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS incident_escalation_policies (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,

  incident_type text NOT NULL,
  severity_min text NOT NULL,

  level integer NOT NULL,
  after_minutes integer NOT NULL,

  target_type text NOT NULL,
  target_ref text NOT NULL,

  enabled boolean NOT NULL DEFAULT true,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS incident_escalation_policies_unique
  ON incident_escalation_policies (tenant_id, incident_type, severity_min, level);

CREATE INDEX IF NOT EXISTS incident_escalation_policies_lookup
  ON incident_escalation_policies (tenant_id, incident_type, enabled, severity_min, after_minutes);

CREATE TABLE IF NOT EXISTS incident_escalations (
  incident_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,

  current_level integer NOT NULL DEFAULT 0,
  last_escalated_at timestamptz,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS incident_escalations_tenant
  ON incident_escalations (tenant_id);
