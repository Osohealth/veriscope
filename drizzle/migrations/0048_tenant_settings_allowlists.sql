-- 0048_tenant_settings_allowlists.sql
-- Allowlist guardrails for escalation targets.

ALTER TABLE tenant_settings
  ADD COLUMN IF NOT EXISTS allowed_email_domains text[] NOT NULL DEFAULT ARRAY[]::text[],
  ADD COLUMN IF NOT EXISTS allowed_webhook_hosts text[] NOT NULL DEFAULT ARRAY[]::text[];
