CREATE TABLE IF NOT EXISTS "tenant_invites" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "email" text NOT NULL,
  "role" text NOT NULL DEFAULT 'VIEWER',
  "token_hash" text NOT NULL UNIQUE,
  "expires_at" timestamptz NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "created_by" uuid NOT NULL,
  "accepted_at" timestamptz,
  "accepted_by_user_id" uuid,
  "revoked_at" timestamptz,
  "revoked_by" uuid
);

CREATE INDEX IF NOT EXISTS "tenant_invites_tenant_email"
  ON "tenant_invites" ("tenant_id", "email");

CREATE INDEX IF NOT EXISTS "tenant_invites_tenant_status"
  ON "tenant_invites" ("tenant_id", "accepted_at", "revoked_at");
