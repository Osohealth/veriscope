CREATE TABLE IF NOT EXISTS "api_keys" (
  "id" varchar PRIMARY KEY DEFAULT gen_random_uuid(),
  "user_id" varchar,
  "organization_id" varchar,
  "key_hash" varchar(255) NOT NULL UNIQUE,
  "name" varchar(100),
  "label" varchar(100),
  "is_active" boolean DEFAULT true,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "revoked_at" timestamptz
);
