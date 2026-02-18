CREATE TABLE IF NOT EXISTS "tenant_users" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "user_id" uuid NOT NULL,
  "email" text NOT NULL,
  "display_name" text,
  "role" text NOT NULL DEFAULT 'VIEWER',
  "status" text NOT NULL DEFAULT 'INVITED',
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "created_by" uuid,
  "revoked_at" timestamptz,
  "revoked_by" uuid
);

CREATE UNIQUE INDEX IF NOT EXISTS "tenant_users_tenant_email"
  ON "tenant_users" ("tenant_id", "email");

CREATE UNIQUE INDEX IF NOT EXISTS "tenant_users_tenant_user"
  ON "tenant_users" ("tenant_id", "user_id");

CREATE INDEX IF NOT EXISTS "tenant_users_tenant_status_created"
  ON "tenant_users" ("tenant_id", "status", "created_at" DESC);
