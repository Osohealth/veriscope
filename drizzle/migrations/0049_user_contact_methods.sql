-- 0049_user_contact_methods.sql
-- User contact methods for escalation routing.

CREATE TABLE IF NOT EXISTS user_contact_methods (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  user_id uuid NOT NULL,
  type text NOT NULL,
  value text NOT NULL,
  label text NULL,
  is_primary boolean NOT NULL DEFAULT false,
  is_verified boolean NOT NULL DEFAULT true,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS user_contact_methods_unique
  ON user_contact_methods (tenant_id, user_id, type, value);

CREATE UNIQUE INDEX IF NOT EXISTS user_contact_methods_primary
  ON user_contact_methods (tenant_id, user_id, type)
  WHERE is_primary = true;

CREATE INDEX IF NOT EXISTS user_contact_methods_tenant_user_type_active
  ON user_contact_methods (tenant_id, user_id, type, is_active);

CREATE INDEX IF NOT EXISTS user_contact_methods_tenant_user_primary_created
  ON user_contact_methods (tenant_id, user_id, is_primary DESC, created_at DESC);
